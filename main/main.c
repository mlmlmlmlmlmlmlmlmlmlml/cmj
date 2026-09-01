#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <math.h>
#include "freertos/FreeRTOS.h"
#include "freertos/task.h"
#include "freertos/semphr.h"
#include "esp_wifi.h"
#include "esp_event.h"
#include "esp_log.h"
#include "nvs_flash.h"
#include "esp_http_server.h"
#include "driver/spi_master.h"
#include "driver/ledc.h"
#include "driver/gpio.h"
#include "esp_timer.h"

/* ===================== Wi-Fi ===================== */
#include "wifi_config.h"

/* ===================== 引脚 ===================== */
#define ADS_MOSI_IO     16
#define ADS_MISO_IO     18
#define ADS_SCK_IO      17
#define ADS_CS_IO       15
#define ADS_DRDY_IO     8
#define ADS_CLKOUT_IO   9

/* ===================== ADS131M04 配置 ===================== */
#define ADS_SPI_HOST    SPI2_HOST
#define ADS_CLK_FREQ    8192000
#define ADS_SCLK_FREQ   8000000

/* 传感器坐标 (cm) */
static const float sensor_x[4] = {5.0f,  5.0f,  55.0f, 55.0f};
static const float sensor_y[4] = {45.0f, 5.0f,  5.0f,  45.0f};

/* ===================== 全局变量 ===================== */
static const char *TAG = "cmj";
static httpd_handle_t g_server = NULL;
static spi_device_handle_t ads_spi = NULL;
static SemaphoreHandle_t g_drdy_sem = NULL;

static volatile float g_adc_raw[4] = {0};
static volatile float g_adc_uv[4]  = {0};
static volatile float g_zero_offset[4] = {0};
static volatile float g_total_uv = 0;
static volatile float g_total_uv_avg = 0;
static volatile float g_cop_x = 30.0f;
static volatile float g_cop_y = 25.0f;
static volatile float g_cop_x_avg = 30.0f;
static volatile float g_cop_y_avg = 25.0f;
static volatile bool  g_tare_pending = false;
static volatile bool  g_tared = false;

static volatile int   g_pga_gain = 32;
static volatile bool  g_gain_pending = false;
static volatile int   g_gain_target = 32;

static volatile int   g_osr = 16384;
static volatile bool  g_osr_pending = false;
static volatile int   g_osr_target = 16384;

static volatile float g_drdy_freq = 0;     /* 实测 DRDY 频率 (Hz) */
static volatile int   g_push_cnt = 0;      /* 已推送帧计数 */
static volatile int   g_uv_cnt = 0;        /* 当前历史数据计数 (0-50) */
static volatile int   g_fifo_catch = 0;    /* FIFO 追赶累计帧计数（测试显示） */
static volatile float g_rms_noise[4] = {0}; /* 最近 50 个平均点的 RMS 噪声 (uV) */
static volatile float g_total_rms_noise = 0;  /* 总电压最近 50 点的 RMS 噪声 (uV) */

#ifdef __XTENSA__
extern const char index_html_start[] asm("_binary_index_html_start");
extern const char index_html_end[]   asm("_binary_index_html_end");
#else
extern const char index_html_start[];
extern const char index_html_end[];
#endif

/* ===================== HTTP 根目录 ===================== */
static esp_err_t root_get_handler(httpd_req_t *req) {
    httpd_resp_set_type(req, "text/html; charset=utf-8");
    size_t len = index_html_end - index_html_start;
    return httpd_resp_send(req, index_html_start, len);
}

/* ===================== WebSocket Handler ===================== */
static esp_err_t ws_handler(httpd_req_t *req) {
    if (req->method == HTTP_GET) {
        ESP_LOGI(TAG, "WebSocket 客户端已连接");
        return ESP_OK;
    }
    httpd_ws_frame_t ws_pkt;
    memset(&ws_pkt, 0, sizeof(ws_pkt));
    ws_pkt.type = HTTPD_WS_TYPE_TEXT;
    if (httpd_ws_recv_frame(req, &ws_pkt, 0) == ESP_OK && ws_pkt.len > 0) {
        uint8_t *buf = calloc(1, ws_pkt.len + 1);
        if (buf) {
            ws_pkt.payload = buf;
            httpd_ws_recv_frame(req, &ws_pkt, ws_pkt.len);
            if (strncmp((char*)buf, "tare", 4) == 0) {
                if (g_uv_cnt >= 50) {
                    g_tare_pending = true;
                    ESP_LOGI(TAG, "收到归零请求");
                } else {
                    ESP_LOGW(TAG, "归零请求已拒绝：历史数据尚未准备完成 (%d/50)", g_uv_cnt);
                }
            } else if (strncmp((char*)buf, "gain:", 5) == 0) {
                int g = atoi((char*)buf + 5);
                if (g == 1 || g == 2 || g == 4 || g == 8 || g == 16 || g == 32 || g == 64 || g == 128) {
                    g_gain_target = g;
                    g_gain_pending = true;
                    ESP_LOGI(TAG, "收到增益切换请求: %d", g);
                }
            } else if (strncmp((char*)buf, "osr:", 4) == 0) {
                int o = atoi((char*)buf + 4);
                if (o == 64 || o == 128 || o == 256 || o == 512 || o == 1024 ||
                    o == 2048 || o == 4096 || o == 8192 || o == 16384) {
                    g_osr_target = o;
                    g_osr_pending = true;
                    ESP_LOGI(TAG, "收到 OSR 切换请求: %d", o);
                }
            }
            free(buf);
        }
    }
    return ESP_OK;
}

/* ===================== WebSocket 推送 ===================== */
struct async_resp_arg {
    httpd_handle_t hd;
};

static void ws_async_send(void *arg) {
    httpd_handle_t hd = (httpd_handle_t)arg;
    char payload[384];
    const char *status_str;
    if (g_tared) {
        status_str = "OK";
    } else if (g_uv_cnt >= 50) {
        status_str = "READY";
    } else {
        status_str = "WAIT";
    }
    snprintf(payload, sizeof(payload),
             "R:%.2f,%.2f,%.2f,%.2f;D:%.2f,%.2f,%.2f,%.2f;T:%.1f;TA:%.1f;C:%.2f,%.2f;CA:%.2f,%.2f;S:%s;G:%d;O:%d;F:%.1f;FC:%d;N:%.1f,%.1f,%.1f,%.1f;TN:%.1f",
             g_adc_raw[0], g_adc_raw[1], g_adc_raw[2], g_adc_raw[3],
             g_adc_uv[0],  g_adc_uv[1],  g_adc_uv[2],  g_adc_uv[3],
             g_total_uv, g_total_uv_avg, g_cop_x, g_cop_y, g_cop_x_avg, g_cop_y_avg,
             status_str, g_pga_gain, g_osr, g_drdy_freq, g_fifo_catch,
                          g_rms_noise[0], g_rms_noise[1], g_rms_noise[2], g_rms_noise[3], g_total_rms_noise);

    httpd_ws_frame_t ws_pkt = {
        .payload = (uint8_t *)payload,
        .len = strlen(payload),
        .type = HTTPD_WS_TYPE_TEXT,
        .final = true,
    };

    size_t max_clients = 4;
    int client_fds[4];
    if (httpd_get_client_list(hd, &max_clients, client_fds) == ESP_OK) {
        for (size_t i = 0; i < max_clients; i++) {
            if (httpd_ws_get_fd_info(hd, client_fds[i]) == HTTPD_WS_CLIENT_WEBSOCKET) {
                httpd_ws_send_frame_async(hd, client_fds[i], &ws_pkt);
            }
        }
    }
}

static esp_err_t trigger_async_send(httpd_handle_t handle) {
    return httpd_queue_work(handle, ws_async_send, handle);
}

static esp_err_t ws_post_handshake_cb(httpd_req_t *req) {
    return trigger_async_send(req->handle);
}

/* ===================== ADS131M04 驱动 ===================== */
static inline int32_t ads_parse24(const uint8_t *p) {
    uint32_t v = ((uint32_t)p[0] << 16) | ((uint32_t)p[1] << 8) | p[2];
    if (v & 0x800000) v |= 0xFF000000;
    return (int32_t)v;
}

static void ads_transact(const uint8_t *tx, uint8_t *rx, size_t len) {
    spi_transaction_t t = {
        .length = len * 8,
        .tx_buffer = tx,
        .rx_buffer = rx,
    };
    spi_device_transmit(ads_spi, &t);
}

static void ads_write_reg(uint8_t addr, uint16_t data) {
    uint8_t tx[18] = {0};
    uint8_t rx[18] = {0};
    uint16_t cmd = 0x6000 | ((addr & 0x3F) << 7);
    tx[0] = (cmd >> 8) & 0xFF;
    tx[1] = cmd & 0xFF;
    tx[3] = (data >> 8) & 0xFF;
    tx[4] = data & 0xFF;
    ads_transact(tx, rx, 18);
}

static void ads_read_frame(int32_t *ch0, int32_t *ch1, int32_t *ch2, int32_t *ch3) {
    uint8_t tx[18] = {0};
    uint8_t rx[18] = {0};
    ads_transact(tx, rx, 18);
    *ch0 = ads_parse24(&rx[3]);
    *ch1 = ads_parse24(&rx[6]);
    *ch2 = ads_parse24(&rx[9]);
    *ch3 = ads_parse24(&rx[12]);
}

static uint16_t gain_to_reg(int gain) {
    int code = 0;
    switch (gain) {
        case 1:   code = 0; break;
        case 2:   code = 1; break;
        case 4:   code = 2; break;
        case 8:   code = 3; break;
        case 16:  code = 4; break;
        case 32:  code = 5; break;
        case 64:  code = 6; break;
        case 128: code = 7; break;
        default:  code = 5; break;
    }
    uint16_t val = 0;
    for (int i = 0; i < 4; i++) {
        val |= (code << (i * 4));
    }
    return val;
}

static uint16_t osr_to_clock_reg(int osr) {
    /* CLOCK 寄存器: 0x03, 默认 0x0F0E
       bit15:12 RESERVED=0
       bit11:8  CH_EN=all 1
       bit7:6   RESERVED=0
       bit5     TBM (1=OSR=64)
       bit4:2   OSR[2:0]
       bit1:0   PWR=10b (HR mode)
    */
    uint16_t reg = 0x0F02; /* all ch on, PWR=HR(10), OSR bits=000 */
    if (osr == 64) {
        reg |= (1 << 5); /* TBM=1 */
    } else {
        int osr_code = 0;
        switch (osr) {
            case 128:  osr_code = 0; break;
            case 256:  osr_code = 1; break;
            case 512:  osr_code = 2; break;
            case 1024: osr_code = 3; break;
            case 2048: osr_code = 4; break;
            case 4096: osr_code = 5; break;
            case 8192: osr_code = 6; break;
            case 16384:osr_code = 7; break;
            default:   osr_code = 3; break;
        }
        reg |= (osr_code << 2);
    }
    return reg;
}

static int get_data_rate_from_osr(int osr) {
    /* fCLKIN=8.192MHz, fMOD=4.096MHz, fDATA = fMOD / OSR */
    return 4096000 / osr;
}

static bool ads131m04_init(void) {
    /* CLKIN: LEDC 8.192 MHz */
    ledc_timer_config_t tcfg = {
        .speed_mode = LEDC_LOW_SPEED_MODE,
        .timer_num = LEDC_TIMER_0,
        .duty_resolution = LEDC_TIMER_1_BIT,
        .freq_hz = ADS_CLK_FREQ,
        .clk_cfg = LEDC_USE_APB_CLK,
    };
    if (ledc_timer_config(&tcfg) != ESP_OK) return false;

    ledc_channel_config_t ccfg = {
        .gpio_num = ADS_CLKOUT_IO,
        .speed_mode = LEDC_LOW_SPEED_MODE,
        .channel = LEDC_CHANNEL_0,
        .timer_sel = LEDC_TIMER_0,
        .duty = 1,
        .hpoint = 0,
    };
    if (ledc_channel_config(&ccfg) != ESP_OK) return false;

    gpio_set_drive_capability(ADS_CLKOUT_IO, GPIO_DRIVE_CAP_0);

    /* DRDY 输入 */
    gpio_config_t drdy_cfg = {
        .pin_bit_mask = (1ULL << ADS_DRDY_IO),
        .mode = GPIO_MODE_INPUT,
        .pull_up_en = GPIO_PULLUP_DISABLE,
        .pull_down_en = GPIO_PULLDOWN_DISABLE,
        .intr_type = GPIO_INTR_NEGEDGE,
    };
    gpio_config(&drdy_cfg);

    /* SPI 总线 */
    spi_bus_config_t buscfg = {
        .mosi_io_num = ADS_MOSI_IO,
        .miso_io_num = ADS_MISO_IO,
        .sclk_io_num = ADS_SCK_IO,
        .quadwp_io_num = -1,
        .quadhd_io_num = -1,
        .max_transfer_sz = 32,
    };
    if (spi_bus_initialize(ADS_SPI_HOST, &buscfg, SPI_DMA_CH_AUTO) != ESP_OK) return false;

    spi_device_interface_config_t devcfg = {
        .clock_speed_hz = ADS_SCLK_FREQ,
        .mode = 1,
        .spics_io_num = ADS_CS_IO,
        .queue_size = 1,
        .cs_ena_pretrans = 2,
        .cs_ena_posttrans = 2,
    };
    if (spi_bus_add_device(ADS_SPI_HOST, &devcfg, &ads_spi) != ESP_OK) return false;

    gpio_set_drive_capability(ADS_MOSI_IO, GPIO_DRIVE_CAP_0);
    gpio_set_drive_capability(ADS_SCK_IO,  GPIO_DRIVE_CAP_0);
    gpio_set_drive_capability(ADS_CS_IO,   GPIO_DRIVE_CAP_0);

    /* 软复位 */
    vTaskDelay(pdMS_TO_TICKS(5));
    uint8_t rst[18] = {0x00, 0x11, 0x00};
    uint8_t rsx[18] = {0};
    ads_transact(rst, rsx, 18);
    vTaskDelay(pdMS_TO_TICKS(10));

    /* 配置寄存器 */
    ads_write_reg(0x03, osr_to_clock_reg(g_osr));
    vTaskDelay(pdMS_TO_TICKS(2));
    ads_write_reg(0x04, gain_to_reg(g_pga_gain));
    vTaskDelay(pdMS_TO_TICKS(2));

    ESP_LOGI(TAG, "ADS131M04 初始化完成, SPI=%dMHz, PGA=%d, OSR=%d",
             ADS_SCLK_FREQ / 1000000, g_pga_gain, g_osr);
    return true;
}

/* ===================== DRDY ISR ===================== */
#ifndef __XTENSA__
#ifdef IRAM_ATTR
#undef IRAM_ATTR
#endif
#define IRAM_ATTR
#endif
static void IRAM_ATTR drdy_isr(void *arg) {
    BaseType_t hp = pdFALSE;
    xSemaphoreGiveFromISR(g_drdy_sem, &hp);
    if (hp) portYIELD_FROM_ISR();
}

/* ===================== ADC 采样任务 ===================== */
static void ads_task(void *pv) {
    if (!ads131m04_init()) {
        ESP_LOGE(TAG, "ADC 初始化失败，任务退出");
        vTaskDelete(NULL);
        return;
    }

    gpio_install_isr_service(0);
    gpio_isr_handler_add(ADS_DRDY_IO, drdy_isr, NULL);

    int64_t accum[4] = {0};
    int avg_cnt = 0;
    int fifo_catch = 0;

    int64_t drdy_times[250] = {0};
    int drdy_idx = 0;
    int drdy_cnt = 0;

    float total_history[50] = {0};
    int total_idx = 0;
    int total_cnt = 0;

    float uv_history[4][50] = {0};
    int uv_idx = 0;


    float cop_x_history[50] = {0};
    float cop_y_history[50] = {0};
    int cop_idx = 0;
    int cop_cnt = 0;

    int data_rate = get_data_rate_from_osr(g_osr);
    int avg_target = data_rate / 50;
    if (avg_target < 1) avg_target = 1;

    ESP_LOGI(TAG, "ADC 采样任务开始, 目标数据率=%d SPS, 平均次数=%d", data_rate, avg_target);

    while (1) {
        if (xSemaphoreTake(g_drdy_sem, pdMS_TO_TICKS(100)) != pdTRUE) continue;

        int64_t now_us = esp_timer_get_time();

        drdy_times[drdy_idx] = now_us;
        drdy_idx = (drdy_idx + 1) % 250;
        if (drdy_cnt < 250) drdy_cnt++;

        /* 读取当前帧 */
        int32_t c0, c1, c2, c3;
        ads_read_frame(&c0, &c1, &c2, &c3);
        accum[0] += c0; accum[1] += c1;
        accum[2] += c2; accum[3] += c3;
        avg_cnt++;

        /* FIFO 追赶 */
        int64_t last_warn_us = 0;   /* 和其他局部变量放一起声明 */
        int extra = 0;
        while (gpio_get_level(ADS_DRDY_IO) == 0 && extra < 3) {
            ads_read_frame(&c0, &c1, &c2, &c3);
            accum[0] += c0; accum[1] += c1;
            accum[2] += c2; accum[3] += c3;
            avg_cnt++;
            extra++;
            fifo_catch++;
            g_fifo_catch++;
        }
        if (extra > 0 && now_us - last_warn_us > 1000000) {
            last_warn_us = now_us;
            ESP_LOGW(TAG, "FIFO 追赶中 (累计 %d 帧)", fifo_catch);
        }

        /* ---- 配置热切换 ---- */
        if (g_gain_pending || g_osr_pending) {
            if (g_gain_pending) {
                g_pga_gain = g_gain_target;
                ads_write_reg(0x04, gain_to_reg(g_pga_gain));
                g_gain_pending = false;
                ESP_LOGI(TAG, "增益已切换为 %d", g_pga_gain);
            }
            if (g_osr_pending) {
                g_osr = g_osr_target;
                ads_write_reg(0x03, osr_to_clock_reg(g_osr));
                g_osr_pending = false;
                data_rate = get_data_rate_from_osr(g_osr);
                avg_target = data_rate / 50;
                if (avg_target < 1) avg_target = 1;
                ESP_LOGI(TAG, "OSR 已切换为 %d, 数据率=%d SPS, 平均=%d",
                         g_osr, data_rate, avg_target);
            }
            /* 旧配置下的样本量纲不同，全部作废 */
            g_tared = false;
            memset((void *)g_zero_offset, 0, sizeof(g_zero_offset));
            accum[0] = accum[1] = accum[2] = accum[3] = 0;
            avg_cnt = 0;
            memset(uv_history, 0, sizeof(uv_history));
            uv_idx = 0;
            g_uv_cnt = 0;
            memset((void *)g_rms_noise, 0, sizeof(g_rms_noise));
            g_total_rms_noise = 0;
            memset(total_history, 0, sizeof(total_history));
            total_idx = 0; total_cnt = 0;
            memset(cop_x_history, 0, sizeof(cop_x_history));
            memset(cop_y_history, 0, sizeof(cop_y_history));
            cop_idx = 0; cop_cnt = 0;
            drdy_idx = 0; drdy_cnt = 0;
            continue;
        }

        if (avg_cnt < avg_target) continue;

        if (drdy_cnt >= 2) {
            int oldest_idx = (drdy_idx - drdy_cnt + 250) % 250;
            int64_t period_us = drdy_times[(drdy_idx - 1 + 250) % 250] - drdy_times[oldest_idx];
            if (period_us > 0) {
                g_drdy_freq = (float)(drdy_cnt - 1) * 1000000.0f / (float)period_us;
            }
        }

        float uv_scale = 2.4e6f / ((float)g_pga_gain * 16777216.0f);
        float raw[4];
        for (int i = 0; i < 4; i++) {
            raw[i] = ((float)accum[i] / (float)avg_cnt) * uv_scale;
            accum[i] = 0;
        }
        avg_cnt = 0;

        /* 归零：必须使用完整的 50 帧历史数据，禁止单帧归零 */
        if (g_tare_pending && g_uv_cnt >= 50) {
            for (int i = 0; i < 4; i++) {
                float sum = 0;
                for (int j = 0; j < 50; j++) {
                    sum += uv_history[i][j];
                }
                g_zero_offset[i] = sum / 50.0f;
            }
            g_tare_pending = false;
            g_tared = true;
            ESP_LOGI(TAG, "已归零 (avg): %.2f, %.2f, %.2f, %.2f uV",
                     g_zero_offset[0], g_zero_offset[1],
                     g_zero_offset[2], g_zero_offset[3]);
        }

        for (int i = 0; i < 4; i++) {
            g_adc_raw[i] = raw[i];
            g_adc_uv[i] = raw[i] - g_zero_offset[i];
        }

        for (int i = 0; i < 4; i++) {
            uv_history[i][uv_idx] = raw[i];
        }
        uv_idx = (uv_idx + 1) % 50;
        if (g_uv_cnt < 50) g_uv_cnt++;

        /* 计算最近 50 个平均点的 RMS 噪声（标准差，单位 uV） */
        if (g_uv_cnt >= 50) {
            for (int i = 0; i < 4; i++) {
                float mean = 0;
                float variance = 0;
                for (int j = 0; j < 50; j++) {
                    mean += uv_history[i][j];
                }
                mean /= 50.0f;
                for (int j = 0; j < 50; j++) {
                    float delta = uv_history[i][j] - mean;
                    variance += delta * delta;
                }
                g_rms_noise[i] = sqrtf(variance / 50.0f);
            }
        }

        /* 计算重心 */
        float total = 0;
        float f[4];
        for (int i = 0; i < 4; i++) {
            f[i] = g_adc_uv[i];
            total += f[i];
        }
        g_total_uv = total;

        total_history[total_idx] = total;
        total_idx = (total_idx + 1) % 50;
        if (total_cnt < 50) total_cnt++;

        if (total_cnt > 0) {
            float sum = 0;
            for (int i = 0; i < total_cnt; i++) {
                sum += total_history[i];
            }
            g_total_uv_avg = sum / total_cnt;
        }

        if (total_cnt >= 50) {
            float mean = 0;
            float variance = 0;
            for (int i = 0; i < 50; i++) {
                mean += total_history[i];
            }
            mean /= 50.0f;
            for (int i = 0; i < 50; i++) {
                float delta = total_history[i] - mean;
                variance += delta * delta;
            }
            g_total_rms_noise = sqrtf(variance / 50.0f);
        }

        if (fabsf(total) > 20.0f) {
            float x = 0, y = 0;
            for (int i = 0; i < 4; i++) {
                x += f[i] * sensor_x[i];
                y += f[i] * sensor_y[i];
            }
            g_cop_x = x / total;
            g_cop_y = y / total;
        } else {
            g_cop_x = 30.0f;
            g_cop_y = 25.0f;
        }

        cop_x_history[cop_idx] = g_cop_x;
        cop_y_history[cop_idx] = g_cop_y;
        cop_idx = (cop_idx + 1) % 50;
        if (cop_cnt < 50) cop_cnt++;

        if (cop_cnt > 0) {
            float sum_x = 0, sum_y = 0;
            for (int i = 0; i < cop_cnt; i++) {
                sum_x += cop_x_history[i];
                sum_y += cop_y_history[i];
            }
            g_cop_x_avg = sum_x / cop_cnt;
            g_cop_y_avg = sum_y / cop_cnt;
        }

        /* 直接触发推送（由 ADC DRDY 驱动，不再使用 20ms 软件定时器） */
        if (g_server) {
            trigger_async_send(g_server);
            g_push_cnt++;
        }
    }
}

/* ===================== HTTP/WebSocket 服务器 ===================== */
static httpd_handle_t start_webserver(void) {
    httpd_config_t config = HTTPD_DEFAULT_CONFIG();
    config.lru_purge_enable = true;
    config.max_open_sockets = 4;

    httpd_handle_t hd = NULL;
    if (httpd_start(&hd, &config) != ESP_OK) return NULL;

    httpd_uri_t uri_root = {
        .uri = "/", .method = HTTP_GET,
        .handler = root_get_handler, .user_ctx = NULL
    };
    httpd_register_uri_handler(hd, &uri_root);

    httpd_uri_t uri_ws = {
        .uri = "/ws", .method = HTTP_GET,
        .handler = ws_handler, .user_ctx = NULL,
        .is_websocket = true,
        .ws_post_handshake_cb = ws_post_handshake_cb,
    };
    httpd_register_uri_handler(hd, &uri_ws);

    ESP_LOGI(TAG, "HTTP/WebSocket 服务器已启动");
    return hd;
}

/* ===================== Wi-Fi 事件 ===================== */
static esp_timer_handle_t g_reconnect_timer = NULL;

static void wifi_reconnect_cb(void *arg) {
    esp_wifi_connect();
}

static void wifi_event_handler(void *arg, esp_event_base_t event_base,
                               int32_t event_id, void *event_data) {
    if (event_base == WIFI_EVENT && event_id == WIFI_EVENT_STA_START) {
        esp_wifi_connect();
    } else if (event_base == WIFI_EVENT && event_id == WIFI_EVENT_STA_DISCONNECTED) {
        ESP_LOGI(TAG, "Wi-Fi 断开，5 秒后重连...");
        esp_timer_start_once(g_reconnect_timer, 5000000);
    } else if (event_base == IP_EVENT && event_id == IP_EVENT_STA_GOT_IP) {
        ip_event_got_ip_t *e = (ip_event_got_ip_t *)event_data;
        ESP_LOGI(TAG, "===================================");
        ESP_LOGI(TAG, "  Wi-Fi 已连接!");
        ESP_LOGI(TAG, "  IP: " IPSTR, IP2STR(&e->ip_info.ip));
        ESP_LOGI(TAG, "  浏览器访问: http://" IPSTR "/", IP2STR(&e->ip_info.ip));
        ESP_LOGI(TAG, "===================================");
        if (!g_server) {
            g_server = start_webserver();
        }
    }
}

static void wifi_init(void) {
    ESP_ERROR_CHECK(esp_netif_init());
    ESP_ERROR_CHECK(esp_event_loop_create_default());
    esp_netif_create_default_wifi_sta();

    wifi_init_config_t cfg = WIFI_INIT_CONFIG_DEFAULT();
    ESP_ERROR_CHECK(esp_wifi_init(&cfg));

    esp_event_handler_instance_t any_id, got_ip;
    ESP_ERROR_CHECK(esp_event_handler_instance_register(
        WIFI_EVENT, ESP_EVENT_ANY_ID, &wifi_event_handler, NULL, &any_id));
    ESP_ERROR_CHECK(esp_event_handler_instance_register(
        IP_EVENT, IP_EVENT_STA_GOT_IP, &wifi_event_handler, NULL, &got_ip));

    wifi_config_t wc = {
        .sta = {
            .ssid = WIFI_SSID,
            .password = WIFI_PASS,
            .threshold.authmode = WIFI_AUTH_WPA2_PSK,
            .sae_pwe_h2e = WPA3_SAE_PWE_BOTH,
        },
    };
    ESP_ERROR_CHECK(esp_wifi_set_mode(WIFI_MODE_STA));
    ESP_ERROR_CHECK(esp_wifi_set_config(WIFI_IF_STA, &wc));
    const esp_timer_create_args_t rc_args = {
        .callback = wifi_reconnect_cb,
        .name = "wifi_reconnect",
    };
    ESP_ERROR_CHECK(esp_timer_create(&rc_args, &g_reconnect_timer));
    ESP_ERROR_CHECK(esp_wifi_start());
    ESP_ERROR_CHECK(esp_wifi_set_ps(WIFI_PS_NONE));
    ESP_LOGI(TAG, "Wi-Fi STA 启动，连接 %s...", WIFI_SSID);
}

/* ===================== 主入口 ===================== */
void app_main(void) {
    esp_err_t ret = nvs_flash_init();
    if (ret == ESP_ERR_NVS_NO_FREE_PAGES || ret == ESP_ERR_NVS_NEW_VERSION_FOUND) {
        ESP_ERROR_CHECK(nvs_flash_erase());
        ret = nvs_flash_init();
    }
    ESP_ERROR_CHECK(ret);

    g_drdy_sem = xSemaphoreCreateBinary();

    wifi_init();
    xTaskCreatePinnedToCore(ads_task, "ads_task", 8192, NULL, 24, NULL, 1);
}
