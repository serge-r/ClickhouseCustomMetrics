package main

import (
	"fmt"
	_ "github.com/ClickHouse/clickhouse-go"
	"github.com/caarlos0/env/v6"
	"github.com/jmoiron/sqlx"
	_ "github.com/lib/pq"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	log "github.com/sirupsen/logrus"
	"io/ioutil"
	"net/http"
	"os/signal"
	"reflect"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"errors"
	"gopkg.in/yaml.v2"
	"os"
)

const driverRegex = "^clickhouse$|^postgre$"
const metricTypeRegex = "^counter|^gauge"
const defaultTimeout = 10 //10 seconds

type Config struct {
	Driver  string   `yaml:"driver"`
	Metrics []Metric `yaml:"metrics"`
}

type Metric struct {
	Name        string            `yaml:"name"`
	Help        string            `yaml:"help"`
	Timeout     time.Duration     `yaml:"timeout"`
	ResultField string            `yaml:"resultField"`
	Labels      map[string]string `yaml:"labels"`
	MetricType  string			   `yaml:"metricType"`
	Query       string            `yaml:"query"`
}

type Options struct {
	LogType        string `env:"LOG_TYPE" envDefault:"text"`
	LogLevel       string `env:"LOG_LEVEL" envDefault:"info"`
	ListenPort     int    `env:"PORT" envDefault:"9246"`
	ListenAddr     string `env:"LISTEN_ADDR" envDefault:"localhost"`
	ConnString     string `env:"DB_CONN_STRING" envDefault:"tcp://127.0.0.1:9000?debug=false"`
	ConfigFileName string `env:"CONFIG_FILE"`
}

func initLog(o *Options) *log.Entry {
	switch strings.ToLower(o.LogType) {
	case "text":
		log.SetFormatter(&log.TextFormatter{
			ForceColors: true,
		})
	case "json":
		log.SetFormatter(&log.JSONFormatter{})

	default:
		log.SetFormatter(&log.TextFormatter{
			ForceColors: true,
		})
	}

	switch strings.ToLower(o.LogLevel) {
	case "debug":
		log.SetLevel(log.DebugLevel)
	case "info":
		log.SetLevel(log.InfoLevel)
	case "warn":
		log.SetLevel(log.WarnLevel)
	case "error":
		log.SetLevel(log.ErrorLevel)
	}

	return log.WithField("context", "deploy")

}

func parseOptions() (*Options, error) {
	options := Options{}
	if err := env.Parse(&options); err != nil {
		return nil, err
	}
	if options.ConfigFileName == "" {
		return nil, errors.New("Please provide CONFIG_FILE env")
	}
	return &options, nil
}

func parseConfig(filepath string) (*Config, error) {
	var cfg Config
	fd, err := os.Open(filepath) // For read access.
	if err != nil {
		return nil, err
	}

	b, err := ioutil.ReadAll(fd)
	if err != nil {
		fd.Close()
		return nil, err
	}
	fd.Close()

	err = yaml.Unmarshal(b, &cfg)
	if err != nil {
		return nil, err
	}

	return &cfg, nil
}

func validateConfig(cfg *Config) error {
	drvRgx := regexp.MustCompile(driverRegex)
	typeRgx := regexp.MustCompile(metricTypeRegex)

	if !(drvRgx.MatchString(cfg.Driver)) {
		return errors.New("check driver string - should be \"clickhouse\" or \"postgre\"")
	}

	if len(cfg.Metrics) == 0 {
		return errors.New("this config have no metrics. What I should do?")
	}

	for _, metric := range cfg.Metrics {
		if len(metric.Name) == 0 {
			return errors.New("I found metric without name. Please check metrics names")
		}

		if metric.Timeout == 0 {
			metric.Timeout = defaultTimeout * time.Second
		}

		if !(typeRgx.MatchString(metric.MetricType)) {
			metric.MetricType = "gauge"
		}

		if len(metric.Query) == 0 {
			return errors.New("this metric %s have no queries")
		}

		if len(metric.ResultField) == 0 {
			return errors.New("this metric created without result filed. Please provide result filed")
		}
	}
	return nil
}

func safeExit(code int) {
	time.Sleep(3)
	os.Exit(code)
}

// Magic function for determine metric type
func getMetricType(metricType string) interface{} {
	var MetricType map[string]reflect.Type = make(map[string]reflect.Type)

	MetricType["counter"] = reflect.TypeOf(prometheus.CounterVec{})
	MetricType["gauge"] = reflect.TypeOf(prometheus.GaugeVec{})

	return MetricType[metricType]
}

func processMetrics(logger *log.Entry, constring string, driver string,  metric Metric, wg *sync.WaitGroup, mu *sync.Mutex) {

		var result = map[string]interface{}{}
		var isRegistered  = false

		logger.Debugf("Start processing metric %s",metric.Name)
		for {
			// Init and clean labelNames
			var labelNames []string

			// Open connection and make query
			logger.Debugf("Trying connect to database %s", constring)
			db, err := sqlx.Open(driver, constring)
			if err != nil {
				logger.Errorf("Cannot connect to database. Error: %v", err)
				wg.Done()
				return
			}
			rows, err := db.Queryx(metric.Query)
			if err != nil {
				logger.Errorf("Cannot make query \"%s\". Error: %v", metric.Query, err)
				wg.Done()
				return
			}

			// Get column names and Init result map
			tmpLabels, err := rows.Columns()
			if err != nil {
				logger.Errorf("Cannot determine columns name. Error: %v", err)
				wg.Done()
				return
			}

			for _, value := range tmpLabels {
				if value != metric.ResultField {
					labelNames = append(labelNames, value)
				}
			}
			for _, item := range labelNames {
				result[item] = ""
			}

			//if metric.MetricType == "counter" {
			//	newMetric = prometheus.NewCounterVec(prometheus.CounterOpts{
			//		Name:        metric.Name,
			//		Help:        metric.Help,
			//		ConstLabels: metric.Labels,
			//	}, labelNames)
			//} else {
			//	newMetric = prometheus.NewGaugeVec(prometheus.GaugeOpts{
			//		Name:        metric.Name,
			//		Help:        metric.Help,
			//		ConstLabels: metric.Labels,
			//	}, labelNames)
			//}

			newMetric := prometheus.NewGaugeVec(prometheus.GaugeOpts{
				Name:        metric.Name,
				Help:        metric.Help,
				ConstLabels: metric.Labels,
			}, labelNames)

			if isRegistered != true {
				mu.Lock()
				logger.Debugf("Try to register new metric with name %v",metric.Name)
				prometheus.MustRegister(newMetric)
				isRegistered = true
				mu.Unlock()
			}

			// Fill label values
			for rows.Next() {
				var labelValues []string
				err = rows.MapScan(result)
				if err != nil {
					logger.Errorf("Cannot scan rows. Error: %v", err)
					wg.Done()
					return
				}
				logger.Debugf("Result is %v", result)
				logger.Debugf("Result field is %s", metric.ResultField)
				for _, value := range labelNames {
					if value != metric.ResultField {
						labelValues = append(labelValues, fmt.Sprint(result[value]))
					}
				}
				switch result[metric.ResultField].(type) {
					case uint8:
						value := float64(result[metric.ResultField].(uint8))
						newMetric.WithLabelValues(labelValues...).Add(value)
					case uint16:
						value := float64(result[metric.ResultField].(uint16))
						newMetric.WithLabelValues(labelValues...).Add(value)
					case uint32:
						value := float64(result[metric.ResultField].(uint32))
						newMetric.WithLabelValues(labelValues...).Add(value)
					case uint64:
						value := float64(result[metric.ResultField].(uint64))
						newMetric.WithLabelValues(labelValues...).Add(value)
					default:
						logger.Errorf("Metric %s have a wrong type result filed %s",metric.Name, metric.ResultField)
						wg.Done()
						return
				}


			}
			rows.Close()
			db.Close()
			time.Sleep(metric.Timeout)
		}
}

func main() {
	var wg sync.WaitGroup
	var mu sync.Mutex

	options, err := parseOptions()
	if err != nil {
		panic(err)
	}
	logger := initLog(options)
	logger.Debugf("Started")
	logger.Debugf("Parsing config")
	config, err := parseConfig(options.ConfigFileName)
	if err != nil {
		panic(err)
	}
	logger.Debugf("Metric fileds is %v", config)
	err = validateConfig(config)
	if err != nil {
		panic(err)
	}

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan,
		syscall.SIGHUP,
		syscall.SIGINT,
		syscall.SIGTERM,
		syscall.SIGKILL,
		syscall.SIGQUIT)

	go func() {
		sig := <-sigChan
		logger.Infof("Exit by signal \"%s\"", sig.String())
		if sig == syscall.SIGKILL {
			safeExit(1)
		} else {
			logger.Infof("Exiting...")
			safeExit(0)
		}
	}()

	logger.Debugf("Processing metrics")
	wg.Add(len(config.Metrics))
	for _, metric := range config.Metrics {
		logger.Debugf("Metric is: %v",metric.Name)
		go processMetrics(logger, options.ConnString, config.Driver, metric, &wg, &mu)
	}

	go func() {
		log.Infof("Start listening on %s:%d", options.ListenAddr, options.ListenPort)
		http.Handle("/metrics", promhttp.Handler())
		http.ListenAndServe(options.ListenAddr+":"+strconv.Itoa(options.ListenPort), nil)
	} ()

	wg.Wait()
	logger.Debug("All processing threads has been exit. Shutdown")
	safeExit(1)
}
