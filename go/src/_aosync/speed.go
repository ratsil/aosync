package main

import (
	"fmt"
	t "time"
)

//Speed .
type Speed struct {
	CurrentStart    t.Time
	OverallStart    t.Time
	CurrentDuration t.Duration
	TotalDuration   t.Duration
	CurrentQty      int64
	OverallQty      int64
}

//Start .
func (th *Speed) Start() {
	th.Restart()
	th.OverallStart = th.CurrentStart
	th.TotalDuration = th.CurrentDuration
	th.OverallQty = th.CurrentQty
}

//Wake .
func (th *Speed) Wake() {
	th.CurrentStart = t.Now()
}

//Sleep .
func (th *Speed) Sleep(nQty int64) {
	d := t.Since(th.CurrentStart)
	th.TotalDuration += d
	th.OverallQty += nQty
	th.CurrentDuration += d
	th.CurrentQty += nQty
	th.Wake()
	return
}

//Restart .
func (th *Speed) Restart() {
	th.Wake()
	th.CurrentDuration = 0
	th.CurrentQty = 0
}

func (th *Speed) speed(n int64, d t.Duration) int64 {
	nSpeed := int64(d / t.Second)
	if 0 < nSpeed {
		nSpeed = (((n * 8) / nSpeed) / 1024 / 1024)
	}
	return nSpeed
}

//Average .
func (th *Speed) Average() t.Duration {
	if 2 > th.CurrentQty {
		return th.CurrentDuration
	}
	return t.Duration(th.CurrentDuration.Nanoseconds()/th.CurrentQty) * t.Nanosecond
}

//String .
func (th *Speed) String() string {
	dOverall := t.Since(th.OverallStart)
	return fmt.Sprintf("current speed at ~%dMbit/s for an average %s while total at ~%dMbit/s for a %s and overall at ~%dMbit/s for a %s", th.speed(th.CurrentQty, th.CurrentDuration), th.Average().String(), th.speed(th.OverallQty, th.TotalDuration), th.TotalDuration.String(), th.speed(th.OverallQty, dOverall), dOverall.String())
}
