package driver

import (
	"net/url"
)

//hdb://myuser:mypassword@localhost:30015?DISTRIBUTION=all&RECONNECT=false

type DsnInfo struct {
	Host, Username, Password string
	ConnectProps             url.Values
}

func parseDSN(dsn string) (*DsnInfo, error) {

	url, err := url.Parse(dsn)
	if err != nil {
		return nil, err
	}

	userName := ""
	password := ""
	if url.User != nil {
		userName = url.User.Username()
		password, _ = url.User.Password()
	}

	return &DsnInfo{Host: url.Host, Username: userName, Password: password,
		ConnectProps: url.Query()}, nil
}
