#include <ctype.h>
#include <errno.h>
#include <fcntl.h>
#include <modbus/modbus.h>
#include <mosquitto.h>
#include <pthread.h>
#include <signal.h>
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/file.h>
#include <sys/stat.h>
#include <unistd.h>

#include "config_parser.h"
#include "filters.h"
#include "log.h"
#include "mqtt_client.h"
#include "request.h"

static int run = 1;

const char logfile_path[] = "/var/log/openmmg.log";
const char pidfile_path[] = "/var/run/openmmg.pid";

void usage() {
    fprintf(logfile, "Usage:\n\topenmmg -c <configfile> [-D] [-d] [-v] [-h]\n");
    fprintf(logfile, "Options:\n");
    fprintf(logfile, "\t-c <configfile>\t\tPath to configuration file.\n");
    fprintf(logfile, "\t-D\t\t\tRun as daemon.\n");
    fprintf(logfile, "\t-d\t\t\tEnable debug mode.\n");
    fprintf(logfile, "\t-v\t\t\tEnable verbose mode.\n");
    fprintf(logfile, "\t-h\t\t\tShow this help.\n");

    exit(EXIT_FAILURE);
}

void handle_signal(int s) {
    run = 0;
}

int daemonize(void) {
    pid_t pid, sid;
    int fd;

    flog(logfile, "[DEBUG] Enter daemonize()\n");

    pid = fork();
    if (pid < 0) {
        flog(logfile, "[DEBUG] fork() failed\n");
        exit(EXIT_FAILURE);
    }

    if (pid > 0) {
        flog(logfile, "[DEBUG] Parent process exits, child PID: %d\n", pid);
        exit(EXIT_SUCCESS);
    }

    umask(0);

    fd = open(pidfile_path, O_RDONLY);
    if (fd >= 0) {
        char pidbuf[16];
        read(fd, pidbuf, sizeof(pidbuf));
        close(fd);
        pid_t pid_from_file = atoi(pidbuf);

        flog(logfile, "[DEBUG] Existing PID file detected: %d\n", pid_from_file);

        if (pid_from_file > 0 && kill(pid_from_file, 0) == 0) {
            flog(logfile, "process already running with PID %d\n", pid_from_file);
            exit(EXIT_FAILURE);
        }

        fd = open(pidfile_path, O_RDWR | O_CREAT, 0640);
        if (fd < 0) {
            flog(logfile, "[DEBUG] unable to write PID file\n");
            exit(EXIT_FAILURE);
        }

        snprintf(pidbuf, sizeof(pidbuf), "%d", getpid());
        write(fd, pidbuf, strlen(pidbuf));
        close(fd);
    } else {
        flog(logfile, "[DEBUG] unable to open PID file\n");
        exit(EXIT_FAILURE);
    }

    set_logfile(logfile_path);
    flog(logfile, "[DEBUG] logfile redirected to %s\n", logfile_path);

    sid = setsid();
    if (sid < 0) {
        flog(logfile, "[DEBUG] setsid() failed\n");
        exit(EXIT_FAILURE);
    }

    if (chdir("/") < 0) {
        flog(logfile, "[DEBUG] chdir('/') failed\n");
        exit(EXIT_FAILURE);
    }

    return 0;
}

int main(int argc, char *argv[]) {
    logfile = stderr;

    // 强制启用文件日志（无论是否 daemon）
    set_logfile(logfile_path);

    flog(logfile, "[DEBUG] Program start\n");

    filter_t *filter_list = NULL;
    config_t config;
    memset(&config, 0, sizeof(config_t));

    int rc = 0;
    struct mosquitto *mosq;

    signal(SIGINT, handle_signal);
    signal(SIGTERM, handle_signal);

    char *configfile = NULL;
    int debug = 0;
    int verbose = 0;
    int daemon = 0;

    int c;
    flog(logfile, "[DEBUG] Parsing command arguments...\n");
    while ((c = getopt(argc, argv, "c:Ddhv")) != -1) {
        switch (c) {
        case 'c':
            configfile = optarg;
            flog(logfile, "[DEBUG] Argument -c detected, file=%s\n", configfile);
            break;
        case 'D':
            daemon = 1;
            flog(logfile, "[DEBUG] Argument -D enable daemon mode\n");
            break;
        case 'd':
            debug = 1;
            flog(logfile, "[DEBUG] Argument -d enable debug\n");
            break;
        case 'v':
            verbose = 1;
            flog(logfile, "[DEBUG] Argument -v enable verbose\n");
            break;
        case 'h':
        default:
            usage();
            return 1;
        }
    }

    /* Default values */
    flog(logfile, "[DEBUG] Setting default config values\n");

    config.mqtt_protocol_version = MQTT_PROTOCOL_V311;
    config.qos = 0;
    config.retain = 0;
    config.keepalive = 60;
    config.port = 1883;
    config.timeout = 10;
    config.reconnect_delay = 5;
    config.verify_ca_cert = 1;

    strncpy(config.host, "localhost", sizeof(config.host) - 1);
    strncpy(config.request_topic, "request", sizeof(config.request_topic) - 1);
    strncpy(config.response_topic, "response", sizeof(config.response_topic) - 1);
    strncpy(config.tls_version, "tlsv1.1", sizeof(config.tls_version) - 1);
    sprintf(config.client_id, "openmmg_client_%d", getpid());

    if (configfile == NULL) {
        char *config_files[] = {
            "/etc/openmmg/openmmg.conf",
            "/etc/openmmg/settings.conf",
            "./openmmg.conf",
            NULL
        };

        int i = 0;
        while (config_files[i] != NULL) {
            flog(logfile, "[DEBUG] Trying config file: %s\n", config_files[i]);

            if (access(config_files[i], F_OK) != -1) {
                flog(logfile, "[DEBUG] File exists, calling config_parse()\n");

                if (config_parse(config_files[i], &config) == 0) {
                    flog(logfile, "[DEBUG] config_parse SUCCESS: %s\n", config_files[i]);
                    break;
                } else {
                    flog(logfile, "[DEBUG] config_parse FAILED on %s\n", config_files[i]);
                }
            }
            i++;
        }

        if (config_files[i] == NULL) {
            flog(logfile, "unable to load config file\n");
            exit(EXIT_FAILURE);
        }

    } else {
        flog(logfile, "[DEBUG] Using user-specified config: %s\n", configfile);

        if (access(configfile, F_OK) != -1) {
            if (config_parse(configfile, &config) != 0) {
                flog(logfile, "unable to load config file\n");
                exit(EXIT_FAILURE);
            }
        }
    }

    /* validate config */
    flog(logfile, "[DEBUG] Validating config...\n");
    int err = validate_config(&config);
    if (err != 0) {
        flog(logfile, "invalid format of config file (%d)\n", err);
        exit(EXIT_FAILURE);
    }

    if (daemon) {
        flog(logfile, "[DEBUG] Entering daemon mode\n");
        if (daemonize() != 0) {
            flog(logfile, "unable to start as daemon\n");
            exit(EXIT_FAILURE);
        }
    }

    flog(logfile, "starting Open MQTT Modbus Gateway\n");

    /* Initialize MQTT */
    flog(logfile, "[DEBUG] mosquitto_lib_init()\n");
    mosquitto_lib_init();

    flog(logfile, "[DEBUG] mosquitto_new client_id=%s\n", config.client_id);
    mosq = mosquitto_new(config.client_id, true, &config);

    if (mosq) {
        flog(logfile, "[DEBUG] mosq non-null, configuring callbacks\n");

        mosquitto_threaded_set(mosq, 1);

        mosquitto_connect_callback_set(mosq, mqtt_connect_callback);
        mosquitto_message_callback_set(mosq, mqtt_message_callback);

        /* username/password */
        if (strlen(config.username) > 0 && strlen(config.password) > 0) {
            flog(logfile, "[DEBUG] Setting MQTT username/password\n");

            if (mosquitto_username_pw_set(mosq, config.username, config.password) != MOSQ_ERR_SUCCESS) {
                flog(logfile, "[DEBUG] Unable to set username/password\n");
                goto terminate;
            }
        }

        /* TLS */
        if (strlen(config.ca_cert_path) > 0) {
            flog(logfile, "[DEBUG] TLS enabled, checking certs\n");

            char *ca_cert = config.ca_cert_path;
            char *cert = NULL;
            char *key = NULL;

            if (strlen(config.cert_path) > 0 && strlen(config.key_path) > 0) {
                cert = config.cert_path;
                key = config.key_path;
                flog(logfile, "[DEBUG] TLS using client cert+key\n");
            }

            int ret = mosquitto_tls_set(mosq, ca_cert, NULL, cert, key, NULL);
            flog(logfile, "[DEBUG] mosquitto_tls_set ret=%d\n", ret);
            if (ret != MOSQ_ERR_SUCCESS) goto terminate;

            flog(logfile, "[DEBUG] Setting TLS version = %s\n", config.tls_version);
            mosquitto_tls_opts_set(mosq, 1, config.tls_version, NULL);
        }

        /* MQTT protocol */
        flog(logfile, "[DEBUG] Setting MQTT protocol version\n");
        mosquitto_opts_set(mosq, MOSQ_OPT_PROTOCOL_VERSION, &config.mqtt_protocol_version);

        /* connect */
        flog(logfile, "[DEBUG] Connecting to broker host=%s port=%d\n",
             config.host, config.port);

        rc = mosquitto_connect(mosq, config.host, config.port, config.keepalive);
        flog(logfile, "[DEBUG] mosquitto_connect rc=%d (%s)\n",
             rc, mosquitto_strerror(rc));

        if (rc) goto terminate;

        /* main loop */
        flog(logfile, "[DEBUG] Entering mosquitto_loop()\n");

        while (run) {
            rc = mosquitto_loop(mosq, -1, 1);
            if (rc != MOSQ_ERR_SUCCESS) {
                flog(logfile, "[DEBUG] mosquitto_loop error: %d (%s)\n", rc, mosquitto_strerror(rc));
            }
            if (rc && run) {
                flog(logfile, "[DEBUG] mosquitto_loop error=%d (%s)\n",
                     rc, mosquitto_strerror(rc));

                sleep(10);
                flog(logfile, "[DEBUG] Attempting reconnect...\n");
                mosquitto_reconnect(mosq);
            }
        }

terminate:
        flog(logfile, "[DEBUG] Terminating MQTT client\n");
        mosquitto_destroy(mosq);
    } else {
        flog(logfile, "[DEBUG] mosquitto_new returned NULL\n");
    }

    mosquitto_lib_cleanup();
    flog(logfile, "[DEBUG] mosquitto_lib_cleanup done\n");

    if (logfile != NULL) {
        flog(logfile, "[DEBUG] Closing logfile\n");
        fclose(logfile);
    }

    return rc;
}
