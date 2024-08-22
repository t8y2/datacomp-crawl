package main

import (
	"bufio"
	"fmt"
	"github.com/spf13/viper"
	"io"
	"log"
	"net/http"
	"net/url"
	"os"
	"path"
	"strconv"
	"strings"
	"sync"
	"time"
)

type Config struct {
	Timeout       int    `yaml:"timeout" mapstructure:"timeout"`
	MaxConcurrent int    `yaml:"max_concurrent" mapstructure:"max_concurrent"`
	ProxyURL      string `yaml:"proxy_url" mapstructure:"proxy_url"`
	UrlsPath      string `yaml:"urls_path" mapstructure:"urls_path"`
	FailPath      string `yaml:"fail_path" mapstructure:"fail_path"`
	StoragePath   string `yaml:"storage_path" mapstructure:"storage_path"`
	LogPath       string `yaml:"log_path" mapstructure:"log_path"`
	UseProxy      bool   `yaml:"use_proxy" mapstructure:"use_proxy"`
}

func getConfig() {
	viper.SetConfigName("crawl.yaml")
	viper.SetConfigType("yaml")
	viper.AddConfigPath(".")
	err := viper.ReadInConfig()
	if err != nil {
		panic(fmt.Errorf("读取配置文件失败: %w", err))
	}
	_ = viper.Unmarshal(&config)
	fmt.Println("配置文件内容:", config)
}

var (
	config           Config
	mu               sync.Mutex
	wg               sync.WaitGroup
	sem              chan struct{}
	httpClient       *http.Client
	failRecords      []string // 存储失败记录
	lastRoundEndTime time.Time
)

type CustomTransport struct {
	Transport http.RoundTripper
}

func (c *CustomTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	// 添加自定义请求头
	req.Header.Add("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/127.0.0.0 Safari/537.36")
	// 调用默认的 RoundTripper 来发送请求
	return c.Transport.RoundTrip(req)
}

func init() {
	getConfig()
	var err error

	// 创建代理 URL
	proxy, err := url.Parse(config.ProxyURL)
	if err != nil {
		fmt.Printf("无法解析代理URL: %s - %v\n", config.ProxyURL, err)
		os.Exit(1)
	}
	if config.UseProxy {
		fmt.Println("使用代理:", config.ProxyURL)
		// 初始化http.Client
		httpClient = &http.Client{
			Transport: &CustomTransport{
				Transport: &http.Transport{
					Proxy: http.ProxyURL(proxy),
					//MaxIdleConnsPerHost:   10,
					MaxIdleConns:          1000,
					IdleConnTimeout:       20 * time.Second,
					TLSHandshakeTimeout:   5 * time.Second,
					ExpectContinueTimeout: 1 * time.Second,
				},
			},
			Timeout: time.Duration(config.Timeout) * time.Second,
		}
	} else {
		fmt.Println("不使用代理")
		// 初始化http.Client
		httpClient = &http.Client{
			Transport: &CustomTransport{
				Transport: &http.Transport{
					//MaxIdleConnsPerHost:   10,
					MaxIdleConns:          1000,
					IdleConnTimeout:       20 * time.Second,
					TLSHandshakeTimeout:   5 * time.Second,
					ExpectContinueTimeout: 1 * time.Second,
				},
			},
			Timeout: time.Duration(config.Timeout) * time.Second,
		}
	}

	// 初始化信号量
	sem = make(chan struct{}, config.MaxConcurrent)
}

func createDirAndFailFile(filename string) {
	storageFilePath := path.Join(config.StoragePath, filename)
	if err := os.MkdirAll(storageFilePath, os.ModePerm); err != nil {
		fmt.Printf("无法创建图片存储目录: %s - %v\n", storageFilePath, err)
		os.Exit(1)
	}
	if err := os.MkdirAll(config.FailPath, os.ModePerm); err != nil {
		fmt.Printf("无法创建失败文件存储目录: %s - %v\n", config.FailPath, err)
		os.Exit(1)
	}
	if err := os.MkdirAll(config.LogPath, os.ModePerm); err != nil {
		fmt.Printf("无法创建日志文件存储目录: %s - %v\n", config.LogPath, err)
		os.Exit(1)
	}
}

func sanitizePath(path string) string {
	// 将路径转换为小写以支持大小写敏感
	lowerPath := strings.ToLower(path)
	// 检查特定文件扩展名并删除其后面的内容
	exts := []string{".jpg", ".jpeg", ".png", ".webp"}
	for _, ext := range exts {
		if idx := strings.Index(lowerPath, ext); idx != -1 {
			return path[:idx+len(ext)] // 只保留扩展名
		}
	}
	return "not-pic"
}

func downloadImage(url, filepath string) {
	resp, err := httpClient.Get(url) // 使用全局的httpClient.Get(url)
	if err != nil {
		writeFailedRecord(url, filepath)
		//fmt.Printf("下载失败:%v\n", err)
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		if resp.StatusCode == 403 || resp.StatusCode == 429 {
			writeFailedRecord(url, filepath)
			return
		}
	}
	// 先将响应体全部读入内存
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		writeFailedRecord(url, filepath)
		//fmt.Printf("读取响应体失败: %s - %v\n", url, err)
		return
	}
	// 检查路径是否合法
	if sanitizedPath := sanitizePath(filepath); sanitizedPath != "not-pic" {
		fileStoragePath := path.Join(config.StoragePath, sanitizedPath)
		// 创建文件并将数据写入文件
		out, err := os.Create(fileStoragePath)
		if err != nil {
			writeFailedRecord(url, filepath)
			fmt.Printf("创建文件失败: %s - %v\n", sanitizedPath, err)
			return
		}
		defer out.Close()

		if _, err := out.Write(body); err != nil {
			writeFailedRecord(url, filepath)
			//fmt.Printf("写入文件失败: %s - %v\n", sanitizedPath, err)
		} else {
			//fmt.Printf("下载完成: %s\n", sanitizedPath)
		}
	}
}

func writeFailedRecord(url, path string) {
	mu.Lock()
	defer mu.Unlock()
	failRecords = append(failRecords, fmt.Sprintf("%s %s", url, path))
}

func writeFailedRecordsToFile(filename string) {
	mu.Lock()
	defer mu.Unlock()
	failFileName := path.Join(config.FailPath, filename+".txt")
	failFile, _ := os.OpenFile(failFileName, os.O_WRONLY|os.O_CREATE, 0644)
	for _, record := range failRecords {
		if _, err := failFile.WriteString(record + "\n"); err != nil {
			fmt.Printf("写入失败记录失败: %v\n", err)
		}
	}
	fmt.Printf("%v: 失败记录写入完成 \n", filename)
	failRecords = nil // 清空内存中的记录
}

func downloadImagesFromFile(filename string) {
	fmt.Printf("开始下载%v\n", filename)
	// 打开文件
	bT := time.Now()
	urlFilePath := path.Join(config.UrlsPath, filename+".txt")
	file, err := os.Open(urlFilePath)
	if err != nil {
		fmt.Printf("无法打开文件: %s - %v\n", filename, err)
		return
	}
	defer file.Close()
	// 读取所有行到内存
	var lines []string
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		lines = append(lines, scanner.Text())
	}
	eT := time.Since(bT) // 从开始到当前所消耗的时间
	fmt.Println("所有行读取完成，耗时: ", eT, "总行数: ", len(lines))

	// 开始处理每一行
	for _, line := range lines {
		parts := strings.SplitN(line, " ", 2)
		if len(parts) < 2 {
			continue
		}
		url, path := parts[0], parts[1]
		path = strings.TrimSpace(path)
		wg.Add(1)
		sem <- struct{}{} // 获取信号量
		go func(url, path string) {
			defer func() {
				wg.Done()
				<-sem
			}()
			downloadImage(url, path)
		}(url, path)
	}
	wg.Wait()

	// 记录当前时间作为这一轮的完成时间
	currentRoundEndTime := time.Now()
	fmt.Printf("本轮下载完成，当前时间：%v\n", currentRoundEndTime)
	// 检查与上一轮的时间间隔
	if !lastRoundEndTime.IsZero() && currentRoundEndTime.Sub(lastRoundEndTime) < 5*time.Minute {
		fmt.Printf("与上一轮的时间间隔不超过5分钟，退出程序。\n")
		os.Exit(0)
	}
	// 更新上一轮的完成时间为当前轮的完成时间
	lastRoundEndTime = currentRoundEndTime
}
func getStartAndEndIndex() (int, int) {
	scanner := bufio.NewScanner(os.Stdin)
	// 提示用户输入起始数字
	fmt.Print("请输入起始数字（例如1，代表文件名00001）: ")
	scanner.Scan()
	startNum, err := strconv.Atoi(scanner.Text())
	if err != nil {
		log.Fatal("无法解析起始数字，请确保输入的是有效的数字")
	}

	// 提示用户输入结束数字
	fmt.Print("请输入结束数字（例如10，代表文件名00010）: ")
	scanner.Scan()
	endNum, err := strconv.Atoi(scanner.Text())
	if err != nil {
		log.Fatal("无法解析结束数字，请确保输入的是有效的数字")
	}
	return startNum, endNum
}

func writeLog(startNum, endNum int, filename string) {
	logFileName := fmt.Sprintf("log-%v-%v.txt", startNum, endNum)
	logFilePath := path.Join(config.LogPath, logFileName)
	logFile, err := os.OpenFile(logFilePath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
	if err != nil {
		log.Fatal("无法打开日志文件: ", err)
	}
	defer logFile.Close()
	log.SetOutput(logFile)
	log.SetFlags(log.LstdFlags | log.Lshortfile)
	// 记录日志
	log.Printf("下载完成: %s\n", filename)
	fmt.Printf("%s: 日志写入完成\n", filename)
}
func main() {

	startNum, endNum := getStartAndEndIndex()
	for i := startNum; i <= endNum; i++ {
		filename := fmt.Sprintf("train-%05d-of-03550", i)
		createDirAndFailFile(filename)
		downloadImagesFromFile(filename)
		// 所有下载任务完成后一次性写入失败记录
		writeFailedRecordsToFile(filename)
		writeLog(startNum, endNum, filename)
	}
}
