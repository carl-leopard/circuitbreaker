package breaker

import (
	"errors"
	"sync"
	"sync/atomic"
	"time"
)

var (
	defaultConfig = Config{
		RefreshInterval:         5 * 60 * 1000,
		ErrorThresholdPercent:   20,
		RequestVolumeThreshold:  1000,
		SuccessThresholdPercent: 85,
		RecoveryInterval:        30 * 1000,
		SuccessVolumeThreshold:  100,
		Callback:                nil,
	}
)

var (
	ErrTooManyErrors           = errors.New("too many errors")
	ErrorManualDroppingRequest = errors.New("manual dropping requests")
)

// type CircuitBreakerStatus int32

// const (
// 	CircuitBreakerStatusClosed CircuitBreakerStatus = iota + 1
// 	CircuitBreakerStatusOpen
// 	CircuitBreakerStatusHalfOpen
// )

const (
	CircuitBreakerStatusClosed int32 = iota + 1
	CircuitBreakerStatusOpen
	CircuitBreakerStatusHalfOpen
)

const (
	maxErrorThresholdPercent uint8 = 100
	minErrorThresholdPercent uint8 = 5
)

// type OperationMode int32

// //circuitBreaker take effect only in auto mode
// const (
// 	OperationModeAuto OperationMode = iota + 1
// 	OperationModeManual
// )

const (
	OperationModeAuto int32 = iota + 1
	OperationModeManual
)

type Config struct {
	RefreshInterval        uint64 //statistical period. unit: ms
	ErrorThresholdPercent  uint8  //circuitBreaker turns to open when errors up to it in refresh interval. take effect with RequestVolumeThreshold
	RequestVolumeThreshold uint32 //circuitBreaker turns to open when errors up to ErrorThresholdPercent and volume comes to it. take effect with ErrorThresholdPercent

	SleepWindow uint32 //after SleepWindow(ms), circuitBreaker turns to half-open when circuitBreaker is open

	SuccessThresholdPercent uint8 //circuitBreaker turns to closed when successes up to it in refresh interval. take effect with SuccessVolumeThreshold
	RecoveryInterval        uint64
	SuccessVolumeThreshold  uint32 //circuitBreaker turns to closed when successes up to SuccessThresholdPercent and volume comes to it. take effect with SuccessThresholdPercent

	Callback func() error //callback when circuitBreak turns to open from closed or to closed from half-open
}

type CircuitBreaker struct {
	status        int32
	operationMode int32

	config Config

	errorVolumeThreshold   uint32
	successVolumeThreshold uint32

	start     time.Time //start of statistical period
	eventTime time.Time //last time at which circuit breaker is open

	requestVolume uint32
	errorVolume   uint32
	mu            sync.Mutex
}

//
func New(config ...Config) *CircuitBreaker {
	breaker := new(CircuitBreaker)
	breaker.status = CircuitBreakerStatusClosed
	breaker.operationMode = OperationModeAuto

	breaker.config = defaultConfig
	if len(config) != 0 {
		breaker.config = config[0]
	}

	if breaker.config.ErrorThresholdPercent > maxErrorThresholdPercent {
		breaker.config.ErrorThresholdPercent = maxErrorThresholdPercent
	}
	if breaker.config.ErrorThresholdPercent < minErrorThresholdPercent {
		breaker.config.ErrorThresholdPercent = minErrorThresholdPercent
	}

	breaker.errorVolumeThreshold = uint32(float32(breaker.config.RequestVolumeThreshold) * (float32)(breaker.config.ErrorThresholdPercent) / (float32)(100))

	breaker.start = time.Now()

	return breaker
}

func (c *CircuitBreaker) SetOperationMode(om int32) {
	atomic.StoreInt32(&c.operationMode, int32(om))
}

func (c *CircuitBreaker) SetStatus(st int32) {
	atomic.StoreInt32(&c.status, int32(st))
}

//ReportRequest is a short hand of ReportRequestN
func (c *CircuitBreaker) ReportRequest() error {
	return c.ReportRequestN(1, time.Now())
}

//ReportRequestN calculates reuqests
func (c *CircuitBreaker) ReportRequestN(n uint32, now time.Time) error {
	return nil
}

//ReportError is a short hand of ReportErrorN
func (c *CircuitBreaker) ReportError() error {
	return c.ReportErrorN(1, time.Now())
}

//ReportErrorN calculates error reuqests
func (c *CircuitBreaker) ReportErrorN(n uint32, now time.Time) error {

	return nil
}

func (c *CircuitBreaker) addRequest(n uint32, now time.Time) error {
	status := atomic.LoadInt32(&c.status)
	switch status {
	case CircuitBreakerStatusOpen:
		return ErrTooManyErrors
	case CircuitBreakerStatusHalfOpen:
		//pass 50 percent request to backend
	case CircuitBreakerStatusClosed:
		//pass all

		atomic.StoreUint32(&c.requestVolume, atomic.AddUint32(&c.requestVolume, n))
	}

	return nil
}

func (c *CircuitBreaker) addErrorRequest(n uint32, now time.Time) error {
	atomic.StoreUint32(&c.errorVolume, atomic.AddUint32(&c.errorVolume, n))
	return nil
}
