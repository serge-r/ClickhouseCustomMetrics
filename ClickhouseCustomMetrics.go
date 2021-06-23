package main

import (
	"fmt"
	_ "github.com/ClickHouse/clickhouse-go"
	"github.com/caarlos0/env/v6"
	"github.com/jmoiron/sqlx"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	log "github.com/sirupsen/logrus"
	"io/ioutil"
	"net/http"
	"os/signal"
	"reflect"
	"strconv"
	"strings"
	"syscall"
	"time"

	"errors"
	"gopkg.in/yaml.v2"
	"os"
)

type Config struct {
	Name  		string 				`yaml:"name"`
	Help  		string 				`yaml:"help"`
	Timeout 	time.Duration		`yaml:"timeout"`
	Driver      string				`yaml:"driver"`
	ResultField string				`yaml:"resultField"`
	Labels  	map[string]string	`yaml:"labels"`
	Query 		string				`yaml:"query"`
}

type Options struct {
	LogType 			 string		  `env:"LOG_TYPE" envDefault:"text"`
	LogLevel  			 string		  `env:"LOG_LEVEL" envDefault:"info"`
	ListenPort			 int		  `env:"PORT" envDefault:"9246"`
	ListenAddr			 string		  `env:"LISTEN_ADDR" envDefault:"localhost"`
	ClickhouseConnString string       `env:"CLICKHOUSE_CONN_STRING" envDefault:"tcp://127.0.0.1:9000?debug=false"`
	ConfigFileName		 string		  `env:"CONFIG_FILE"`
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
		return nil,errors.New("Please provide CONFIG_FILE env")
	}
	return &options, nil
}

func parseConfig(filepath string) ([]Config, error) {
	cfg := []Config{}
	config, err := os.Open(filepath) // For read access.
	if err != nil {
		return nil,err
	}

	b,err := ioutil.ReadAll(config)
	if err != nil {
		config.Close()
		return nil,err
	}
	config.Close()

	err = yaml.Unmarshal(b,&cfg)
	if err != nil {
		return nil,err
	}

	return cfg,nil
}

func safeExit(code int) {
	time.Sleep(3)
	os.Exit(code)
}

func main() {
	options,err:= parseOptions()
	if err != nil {
		panic(err)
	}
	logger := initLog(options)
	logger.Debugf("Started")
	logger.Debugf("Parsing config")
	config,err := parseConfig(options.ConfigFileName)
	if err != nil {
		panic(err)
	}
	logger.Debugf("Config fileds is %v",config)

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan,
		syscall.SIGHUP,
		syscall.SIGINT,
		syscall.SIGTERM,
		syscall.SIGKILL,
		syscall.SIGQUIT)

	// Signal processing
	go func() {
		sig := <-sigChan
		logger.Infof("Exit by signal \"%s\"",sig.String())
		if sig == syscall.SIGKILL {
			safeExit(1)
		} else {
			safeExit(0)
		}
	}()

	logger.Debugf("Creating metrics")
	for _,metric := range config {

		go func() {
			var (
				result       = map[string]interface{}{}
				isRegistered = false
			)

			for {
				// Init and clean labelNames
				labelNames := []string{}

				// Open connection and make query
				logger.Debugf("Trying to connect to database %s",options.ClickhouseConnString)
				connect, err := sqlx.Open(metric.Driver, options.ClickhouseConnString)
				if err != nil {
					logger.Errorf("Cannot connect to database. Error: %v",err)
					return
				}
				rows,err := connect.Queryx(metric.Query)
				if err != nil {
					logger.Errorf("Cannot make query \"%s\". Error: %v",metric.Query,err)
					return
				}

				// Get column names and Init result map
				tmpLabels,err := rows.Columns()
				if err != nil {
					logger.Errorf("Cannot determine columns name. Error: %v",err)
					return
				}
				for _,value := range tmpLabels {
					if value != metric.ResultField {
						labelNames = append(labelNames, value)
					}
				}
				for _,item := range labelNames  {
					result[item] = ""
				}

				newMetric := prometheus.NewCounterVec(prometheus.CounterOpts{
					Name: metric.Name,
					Help: metric.Help,
					ConstLabels: metric.Labels,
				},labelNames)

				if isRegistered != true {
					prometheus.MustRegister(newMetric)
					isRegistered = true
				}

				// Fill label values
				for rows.Next() {
					labelValues := []string{}
					err = rows.MapScan(result)
					if err != nil {
						logger.Errorf("Cannot scan rows. Error: %v",err)
						return
					}
					logger.Debugf("Result is %v",result)
					logger.Debugf("Result field is %s",metric.ResultField)
					logger.Debugf("Type of result: %T",reflect.TypeOf(result[metric.ResultField]))
					for _,value := range labelNames {
						if value != metric.ResultField {
							labelValues = append(labelValues, fmt.Sprint(result[value]))
						}
					}
					newMetric.WithLabelValues(labelValues...).Add(float64(result[metric.ResultField].(uint8)))
				}

				connect.Close()
				time.Sleep(metric.Timeout)
			}
		}()

	}
	log.Infof("Starting listen on %s:%d",options.ListenAddr,options.ListenPort)
	http.Handle("/metrics", promhttp.Handler())
	http.ListenAndServe(options.ListenAddr+":"+strconv.Itoa(options.ListenPort), nil)
}
