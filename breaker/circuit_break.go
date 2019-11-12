package breaker

import (
	"errors"
	"fmt"
	"sync/atomic"
	"time"
)

var (
	defaultOpenConfig = CircuitBreakerOpenConfig{
		RefreshInterval:        3 * time.Minute,
		ErrorThresholdPercent:  20,
		RequestVolumeThreshold: 1000,

		errorVolumeThreshold: uint32(float32(1000) * (float32(20) / float32(100))),
	}

	defaultCloseConfig = CircuitBreakerCloseConfig{
		RecoveryInterval:       time.Minute,
		SuccessVolumeThreshold: 100,
	}
)

var (
	errTooManyErrors        = errors.New("too many errors")
	errCircuitBreakerClosed = errors.New("circult breaker is closed")
)

var (
	errUnknownStatus = errors.New("unknown status")
)

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

	errorVolumeThreshold uint32 //RequestVolumeThreshold * (ErrorThresholdPercent / 100), use to accelerate compare when status is closed
}

//CircuitBreakerCloseConfig case in which circuit breaker turns to closed.
type CircuitBreakerCloseConfig struct {
	RecoveryInterval       time.Duration //circuitBreaker turns to closed when time is end and all of them are success.
	SuccessVolumeThreshold uint32        //circuitBreaker turns to closed when volume comes to it and all of them are success.
}

type CircuitBreakerOption func(c *CircuitBreaker)

func WithOpenConfig(oc CircuitBreakerOpenConfig) CircuitBreakerOption {
	return func(c *CircuitBreaker) {
		oc.errorVolumeThreshold = uint32(float32(oc.RequestVolumeThreshold) * (float32(oc.ErrorThresholdPercent) / float32(100)))
		c.openConfig = oc
	}
}

func WithCloseConfig(cc CircuitBreakerCloseConfig) CircuitBreakerOption {
	return func(c *CircuitBreaker) {
		c.closeConfig = cc
	}
}

func WithSleepWindow(t time.Duration) CircuitBreakerOption {
	return func(c *CircuitBreaker) {
		c.sleepWindow = t
	}
}

func WithCallback(f func()) CircuitBreakerOption {
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

	closeConfig CircuitBreakerCloseConfig
	//successVolume uint32

	callback func() //callback when circuitBreak turns to open from closed or to closed from half-open

	closeChan chan struct{}
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

		closeChan: make(chan struct{}),
	}

	for _, opt := range opts {
		opt(c)
	}

	go c.resetRefreshInterval()

	return c
}

//Close closes circuit breaker
func (c *CircuitBreaker) Close() {
	close(c.closeChan)
}

//ReportRequest is a short hand of ReportRequestN, call when receive a request
func (c *CircuitBreaker) ReportRequest() error {
	select {
	case <-c.closeChan:
		return errCircuitBreakerClosed
	default:
	}

	return c.ReportRequestN(1)
}

//ReportRequestN calculates reuqests
func (c *CircuitBreaker) ReportRequestN(n uint32) error {
	select {
	case <-c.closeChan:
		return errCircuitBreakerClosed
	default:
	}

	return c.addRequest(n)
}

//ReportError is a short hand of ReportErrorN, call when receiving no response from backend or other define error
func (c *CircuitBreaker) ReportError() error {
	select {
	case <-c.closeChan:
		return errCircuitBreakerClosed
	default:
	}

	return c.ReportErrorN(1)
}

//ReportErrorN calculates error reuqests
func (c *CircuitBreaker) ReportErrorN(n uint32) error {
	select {
	case <-c.closeChan:
		return errCircuitBreakerClosed
	default:
	}

	c.addErrorRequest(n)
	return nil
}

func (c *CircuitBreaker) addRequest(n uint32) error {
	status := atomic.LoadInt32(&c.status)
	switch status {
	case CircuitBreakerStatusOpen:
		return errTooManyErrors
	case CircuitBreakerStatusHalfOpen:
		//pass request to backend

		atomic.StoreUint32(&c.requestVolume, atomic.AddUint32(&c.requestVolume, n))
	case CircuitBreakerStatusClosed:
		//pass all

		atomic.StoreUint32(&c.requestVolume, atomic.AddUint32(&c.requestVolume, n))
	default:
		panic(errUnknownStatus)
	}

	return nil
}

func (c *CircuitBreaker) addErrorRequest(n uint32) {
	if n == 0 {
		return
	}

	status := atomic.LoadInt32(&c.status)
	switch status {
	case CircuitBreakerStatusOpen:
		//skip
	case CircuitBreakerStatusHalfOpen:
		atomic.StoreInt32(&c.status, CircuitBreakerStatusOpen)

		go c.waitForSleepWindow()
	case CircuitBreakerStatusClosed:
		v := atomic.AddUint32(&c.errorVolume, n)

		//closed => open
		if v >= c.openConfig.errorVolumeThreshold &&
			atomic.LoadUint32(&c.openConfig.RequestVolumeThreshold) <= atomic.LoadUint32(&c.requestVolume) &&
			v >= c.getCurErrorQuorm() {
			atomic.StoreInt32(&c.status, CircuitBreakerStatusOpen)

			go c.waitForSleepWindow()
			return
		}

		//stay closed
		atomic.StoreUint32(&c.errorVolume, v)
	default:
		panic(errUnknownStatus)
	}
}

func (c *CircuitBreaker) resetRefreshInterval() {
	t := time.NewTicker(c.openConfig.RefreshInterval)
	for {
		select {
		case <-t.C:
			atomic.StoreUint32(&c.requestVolume, 0)
			atomic.StoreUint32(&c.errorVolume, 0)
		case <-c.closeChan:
			fmt.Println("circuit breaker has already exited")

			t.Stop()
			return
		}
	}
}

func (c *CircuitBreaker) waitForSleepWindow() {
	timer := time.NewTimer(c.sleepWindow)

	select {
	case <-timer.C:
		atomic.StoreInt32(&c.status, CircuitBreakerStatusHalfOpen)

		timer.Stop()
	case <-c.closeChan:
		fmt.Println("circuit breaker has already exited")

		timer.Stop()
	}
}

func (c *CircuitBreaker) getCurErrorQuorm() uint32 {
	return uint32(float32(atomic.LoadUint32(&c.requestVolume)) * (float32(c.openConfig.ErrorThresholdPercent) / float32(100)))
}
