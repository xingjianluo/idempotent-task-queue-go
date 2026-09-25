package queue

import "time"

// Clock 时钟抽象，便于测试中手动推进时间。
type Clock interface {
	NowMillis() int64
}

type realClock struct{}

func (realClock) NowMillis() int64 { return time.Now().UnixMilli() }

// NewRealClock 返回系统真实时钟。
func NewRealClock() Clock { return realClock{} }

// FakeClock 测试用可控时钟。
type FakeClock struct {
	millis int64
}

func NewFakeClock(millis int64) *FakeClock   { return &FakeClock{millis: millis} }
func (c *FakeClock) NowMillis() int64        { return c.millis }
func (c *FakeClock) Advance(d time.Duration) { c.millis += d.Milliseconds() }
