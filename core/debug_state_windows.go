package core

import "fmt"

// platformStateLocked is the Windows part of the debug state. Callers hold
// c.mu.
func (c *Connection) platformStateLocked() string {
	return fmt.Sprintf("readArmed=%v writeInFlight=%v outstanding=%d failure=%v",
		c.readArmed, c.writeInFlight, c.outstanding.Load(), c.failure)
}
