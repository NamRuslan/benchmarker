package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"text/tabwriter"
	"time"
)

type Result struct {
	status   string
	duration time.Duration
}

const getMethod = "GET"
const postMethod = "POST"
const patchMethod = "PATCH"
const deleteMethod = "DELETE"
const putMethod = "PUT"

var availableMethods = []string{getMethod, postMethod, patchMethod, deleteMethod, putMethod}

var logger *slog.Logger
var isDebug bool

var url string
var method string
var graphQlPath string
var sessionId string

var rps float64

var requestCount int64

var body []byte

func main() {
	// Флаги с дефолтами
	urlFlag := flag.String("url", "http://localhost:8080", "URL на который будут отправляться запросы.")
	methodFlag := flag.String("m", getMethod, "Метод запроса. Поддерживаемые варианты: GET, POST, PATCH, DELETE, PUT.")
	vuCount := flag.Int("vu", 1, "Кол-во виртуальных пользователей, столько клиентов будут отправлять запросы параллельно.")
	rpsFlag := flag.Float64("rps", 1.0, "Количество запросов в секунду для ОДНОГО виртуального пользователя. Общий целевой RPS будет рассчитываться как rps * vu.")
	requestCountFlag := flag.Int64("rc", 20, "Суммарное/предельное кол-во запросов, отправляемых по URL.")
	testDuration := flag.Int("d", 60, "Длительность теста в секундах.")
	isUseBody := flag.Bool("b", false, "Использовать ли в запросе body . Если используется, нужно положить файл с готовым запросом body.txt рядом с exe файлом. Если это запрос GraphQL, в файл положить просто GraphQL query.")
	isDebugFlag := flag.Bool("debug", false, "Режим дебага. Выводит дополнительную информацию в консоль и сохраняет все в отдельный файл рядом с exe файлом.")

	// Необязательные флаги без дефолтов
	graphQlPathFlag := flag.String("gpath", "", "Эндпоинт GraphQL. Если указан, скрипт будет искать запрос в файле body.txt рядом с собой и автоматически устанавливает method=POST. Пример: \"/some-service/api/v1/graphql\".")
	sessionIdHeaderFlag := flag.String("sid", "", "Хедер ID сессии.")

	flag.Parse()

	url = *urlFlag
	method = *methodFlag
	graphQlPath = *graphQlPathFlag
	sessionId = *sessionIdHeaderFlag
	rps = *rpsFlag
	requestCount = *requestCountFlag

	if *isDebugFlag {
		isDebug = *isDebugFlag
		logFile := getLogFile()
		defer logFile.Close()

		logger = slog.New(slog.NewTextHandler(io.MultiWriter(os.Stdout, logFile), nil))
	} else {
		logger = slog.New(slog.NewTextHandler(os.Stdout, nil))
	}

	if rps <= 0 || *vuCount <= 0 || requestCount <= 0 || *testDuration <= 0 {
		logger.Error("Значения rps, vu, rc и d должны быть больше 0")
		os.Exit(1)
	}

	if !strings.HasPrefix(url, "http://") && !strings.HasPrefix(url, "https://") {
		logger.Error("Не указан протокол http/https для URL. Пример http://localhost:8080")
		os.Exit(1)
	}

	if !slices.Contains(availableMethods, method) {
		logger.Error("Недопустимый метод", "method", method)
		os.Exit(1)
	}

	if *isUseBody || graphQlPath != "" {
		parsed, err := readQueryFromFile()
		if err != nil {
			logError("Невозможно прочитать файл с запросом \"body.txt\"", err)
			os.Exit(1)
		}
		body = parsed
	}

	if graphQlPath != "" {
		method = postMethod

		marshaled, err := json.Marshal(struct {
			Query string `json:"query"`
		}{
			Query: string(body),
		})

		if err != nil {
			logError("Не удалось прочитать файл с телом запроса body.txt", err)
			os.Exit(1)
		}

		body = marshaled
	}

	client := &http.Client{
		Timeout: 30 * time.Second,
		Transport: &http.Transport{
			MaxIdleConns:        *vuCount * 2,
			MaxIdleConnsPerHost: *vuCount * 2,
			IdleConnTimeout:     30 * time.Second,
		},
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	ctx, cancel := context.WithTimeout(ctx, time.Duration(*testDuration)*time.Second)
	defer cancel()

	fmt.Println("==================================================== Начало тестирования ====================================================")
	fmt.Printf("Метод: %s, URL: %s, VUs: %d, RPS: %f, Максимум запросов: %d\n", method, url+graphQlPath, *vuCount, rps, requestCount)
	if len(body) > 0 {
		fmt.Printf("Тело запроса: %q\n\n", body)
	}

	results := make(map[string][]time.Duration)
	resultsChan := make(chan Result, 20)
	resultsDone := make(chan struct{})

	go runResultCollector(results, resultsChan, resultsDone)

	var sentCount atomic.Int64
	var vuWg sync.WaitGroup
	start := time.Now()

	for i := 0; i < *vuCount; i++ {
		vuWg.Add(1)
		go runVU(&vuWg, ctx, &sentCount, client, body, resultsChan)
	}

	vuWg.Wait()
	close(resultsChan)
	<-resultsDone

	writeResult(start, results)
}

func runResultCollector(results map[string][]time.Duration, resultsChan chan Result, resultsDone chan struct{}) {
	for r := range resultsChan {
		results[r.status] = append(results[r.status], r.duration)
	}
	close(resultsDone)
}

func runVU(vuWg *sync.WaitGroup, ctx context.Context, sentCount *atomic.Int64, client *http.Client, body []byte, resultsChan chan Result) {
	var reqWg sync.WaitGroup

	defer vuWg.Done()
	defer reqWg.Wait()

	interval := time.Duration(float64(time.Second) / rps)
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			current := sentCount.Add(1)
			if current > requestCount {
				return
			}
			reqWg.Add(1)
			go sendRequest(ctx, &reqWg, client, body, current, resultsChan)
		}
	}
}

func sendRequest(ctx context.Context, reqWg *sync.WaitGroup, client *http.Client, body []byte, current int64, resultsChan chan Result) {
	defer reqWg.Done()
	reqStart := time.Now()
	resp, err := client.Do(createRequest(ctx, method, url, graphQlPath, sessionId, body))
	since := time.Since(reqStart)

	if err != nil {
		// Таймаут всего теста или выход через ctrl+c, в таком случае подвисшие запросы после внепланового окончания теста не попадут в отчет
		if ctx.Err() != nil {
			return
		}
		var netErr net.Error
		if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, os.ErrDeadlineExceeded) || errors.As(err, &netErr) && netErr.Timeout() {
			logInfo(fmt.Sprintf("#%d Запрос упал по таймауту", current))
			resultsChan <- Result{status: "Timeout", duration: since}
		} else {
			logError(fmt.Sprintf("#%d Ошибка при выполнении запроса", current), err)
			resultsChan <- Result{status: "Failed", duration: since}
		}
		return
	}

	currentBody, err := io.ReadAll(resp.Body)
	resp.Body.Close()

	var status string
	if err != nil {
		if isDebug {
			logError(fmt.Sprintf("#%d Не удалось прочитать содержание body запроса.", current), err)
		}
		status = resp.Status
	} else if graphQlPath != "" {
		if resp.StatusCode >= 400 {
			status = resp.Status
		} else if strings.Contains(string(currentBody), "\"errors\"") {
			status = "GraphQL error"
		} else {
			status = "GraphQL OK"
		}
	} else {
		status = resp.Status
	}
	logInfo(fmt.Sprintf("#%d Запрос выполнен", current), "status", status, "duration", since, "body", string(currentBody))
	resultsChan <- Result{status: status, duration: since}
}

func writeResult(start time.Time, results map[string][]time.Duration) {
	totalDuration := time.Since(start)
	var reqsTotal int
	w := tabwriter.NewWriter(os.Stdout, 0, 0, 3, ' ', 0)

	fmt.Printf("\n==================================================== Тест окончен ====================================================\n")
	fmt.Fprintf(w, "Результат запроса\tКол-во запросов\tСреднее арифметическое\tp50(медиана)\tp90\tp95")

	for code := range results {
		codeResultsLen := len(results[code])
		reqsTotal += codeResultsLen
		var codeDurationTotal time.Duration
		for _, d := range results[code] {
			codeDurationTotal += d
		}

		avg := codeDurationTotal / time.Duration(codeResultsLen)

		slices.Sort(results[code])
		p50 := results[code][codeResultsLen/2].Round(time.Millisecond)
		p90 := results[code][int(float64(codeResultsLen)*0.9)].Round(time.Millisecond)
		p95 := results[code][int(float64(codeResultsLen)*0.95)].Round(time.Millisecond)

		fmt.Fprintf(w, "\n%s\t%d\t%v\t%v\t%v\t%v", code, codeResultsLen, avg.Round(time.Millisecond), p50, p90, p95)
	}
	w.Flush()
	fmt.Printf("\n\nОбщее количество запросов: %d", reqsTotal)
	fmt.Printf("\nОбщее время: %v\n", totalDuration.Round(100*time.Millisecond))
}

func logInfo(message string, args ...any) {
	if isDebug {
		logger.Info(message, args...)
	} else {
		fmt.Println(message)
	}
}

func logError(message string, err error) {
	if isDebug {
		logger.Error(message, "err", err.Error())
	} else {
		fmt.Println(message + " | Ошибка: " + err.Error())
	}
}

func createRequest(ctx context.Context, method, url, graphQlPath, sessionIdHeader string, body []byte) *http.Request {
	req, err := http.NewRequestWithContext(ctx, method, url+graphQlPath, bytes.NewReader(body))
	if err != nil {
		logError("Невозможно сгенерировать запрос", err)
		os.Exit(1)
	}

	req.Header.Add("Accept", "*/*")

	if len(body) > 0 {
		req.Header.Add("Content-Type", "application/json")
	}

	if sessionIdHeader != "" {
		req.Header.Add("Cookie", fmt.Sprintf("sessionId=%s", sessionIdHeader))
	}
	return req
}

func getLogFile() *os.File {
	fileName := time.Now().Format("02-01-2006_15-04-05") + "-benchmarker_log.txt"
	logFile, err := os.Create(fileName)
	if err != nil {
		fmt.Printf("Невозможно создать файл лога при локальном запуске. err: %v\n", err.Error())
		os.Exit(1)
	}
	return logFile
}

func readQueryFromFile() ([]byte, error) {
	if exePath, err := os.Executable(); err == nil {
		exeDir := filepath.Dir(exePath)
		if data, err := os.ReadFile(filepath.Join(exeDir, "body.txt")); err == nil {
			return data, nil
		}
	}
	// При запуске через go run будет искать файл рядом
	return os.ReadFile("body.txt")
}
