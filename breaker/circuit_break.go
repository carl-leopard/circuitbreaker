package breaker

import (
	"errors"
	"sync"
	"sync/atomic"
	"time"
)

var (
	defaultOpenConfig = CircuitBreakerOpenConfig{
		RefreshInterval:        5 * 60 * 1000,
		ErrorThresholdPercent:  20,
		RequestVolumeThreshold: 1000,
	}

	defaultCloseConfig = CircuitBreakerCloseConfig{
		SuccessThresholdPercent: 85,
		RecoveryInterval:        30 * 1000,
		SuccessVolumeThreshold:  100,
	}
)

var (
	ErrTooManyErrors           = errors.New("too many errors")
	ErrorManualDroppingRequest = errors.New("manual dropping requests")
)

const (
	minRefreshInterval = time.Minute
	maxRefreshInterval = time.Minute * 5

	minSleepWindow = time.Second * 15
	maxSleepWindow = minRefreshInterval

	minRecoveryInterval = time.Second * 30
	maxRecoveryInterval = maxRefreshInterval
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

//CircuitBreakerOpenConfig case in which circuit breaker turns to open
type CircuitBreakerOpenConfig struct {
	RefreshInterval        time.Duration //statistical period. unit: ms
	ErrorThresholdPercent  uint8         //circuitBreaker turns to open when errors up to it in refresh interval. take effect with RequestVolumeThreshold
	RequestVolumeThreshold uint32        //circuitBreaker turns to open when errors up to ErrorThresholdPercent and volume comes to it. take effect with ErrorThresholdPercent

	errorVolumeThreshold uint32 //RequestVolumeThreshold * (ErrorThresholdPercent / 100)
}

//CircuitBreakerCloseConfig case in which circuit breaker turns to closed
type CircuitBreakerCloseConfig struct {
	RecoveryInterval        time.Duration
	SuccessThresholdPercent uint8  //circuitBreaker turns to closed when successes up to it in refresh interval. take effect with SuccessVolumeThreshold
	SuccessVolumeThreshold  uint32 //circuitBreaker turns to closed when successes up to SuccessThresholdPercent and volume comes to it. take effect with SuccessThresholdPercent

	successVolumeThreshold uint32 //SuccessVolumeThreshold * (SuccessThresholdPercent / 100)
}

type CircuitBreakerOption func(c *CircuitBreaker)

func SetOpenConfig(oc CircuitBreakerOpenConfig) CircuitBreakerOption {
	return func(c *CircuitBreaker) {
		if oc.RefreshInterval >= minRefreshInterval && oc.RefreshInterval <= maxRefreshInterval {
			c.openConfig = oc
		}
	}
}

func SetCloseConfig(cc CircuitBreakerCloseConfig) CircuitBreakerOption {
	return func(c *CircuitBreaker) {
		if cc.RecoveryInterval >= minRecoveryInterval && cc.RecoveryInterval <= maxRecoveryInterval {
			c.closeConfig = cc
		}
	}
}

func SetSleepWindow(t time.Duration) CircuitBreakerOption {
	return func(c *CircuitBreaker) {
		if t >= minSleepWindow && t <= maxSleepWindow {
			c.sleepWindow = t
		}
	}
}

func SetCallback(f func()) CircuitBreakerOption {
	return func(c *CircuitBreaker) {
		if f != nil {
			c.callback = f
		}
	}
}

//CircuitBreaker
type CircuitBreaker struct {
	status        int32
	requestVolume uint32 //total num of request

	openConfig  CircuitBreakerOpenConfig
	errorVolume uint32

	sleepWindow time.Duration //after SleepWindow, circuitBreaker turns to half-open when circuitBreaker is open

	closeConfig   CircuitBreakerCloseConfig
	successVolume uint32

	callback func() //callback when circuitBreak turns to open from closed or to closed from half-open

	start     time.Time //start of statistical period
	eventTime time.Time //last time at which circuit breaker is open

	mu sync.Mutex
}

//New return a new citcuit breaker
func New(opts ...CircuitBreakerOption) *CircuitBreaker {
	c := &CircuitBreaker{
		status:        CircuitBreakerStatusClosed,
		requestVolume: 0,

		openConfig:  defaultOpenConfig,
		errorVolume: 0,

		sleepWindow: time.Minute * 3,

		closeConfig: defaultCloseConfig,

		callback: nil,

		start: time.Now(),
	}

	for _, opt := range opts {
		opt(c)
	}

	return c
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
