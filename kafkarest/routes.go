package main

import (
	"net/http"
)

// Route structure to contain our routing paths
type Route struct {
	Name        string
	Method      string
	Pattern     string
	HandlerFunc http.HandlerFunc
}

// Routes - array of all known routing paths
type Routes []Route

var routes = Routes{
	Route{
		"PostV1Syslog",
		"POST",
		"/v1/syslog",
		PostV1Syslog,
	},
	Route{
		"PostLogChannel",
		"POST",
		"/v1/syslog/{channel}",
		PostV1SyslogChannel,
	},
}
