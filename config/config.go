package config

import (
	"encoding/json"
	"fmt"
	"os"
	"strings"

	"github.com/BurntSushi/toml"
	"github.com/tidwall/pretty"
)

// DBConfig is database configuration.
type DBConfig struct {
	Host        string `toml:"host" json:"host"`
	Port        int    `toml:"port" json:"port"`
	User        string `toml:"user" json:"user"`
	Password    string `toml:"password" json:"-"` // omit it for privacy
	DBName      string `toml:"db-name" json:"db-name"`
	SessionVars string `toml:"session-variables" json:"session-variables"`
}

type Config struct {
	DBConfig `toml:"db-config" json:"db-config"`
	Thread   int `toml:"thread" json:"thread"`
}

func (c *Config) Load(path string) {
	if path == "" {
		return
	}
	_, err := toml.DecodeFile(path, c)
	if err != nil {
		fmt.Fprintln(os.Stderr, err.Error())
		os.Exit(1)
	}
}

func (c *Config) GetSessionVars() []string {
	if c.SessionVars == "" {
		return nil
	}
	vars := strings.Split(c.SessionVars, ",")
	for i, item := range vars {
		if item == "" {
			continue
		}
		fields := strings.Split(item, "=")
		if len(fields) != 2 {
			fmt.Printf("invalid session variable %s, format is var_name=value\n", item)
			os.Exit(-1)
		}
		name := strings.TrimSpace(fields[0])
		value := strings.TrimSpace(fields[1])
		vars[i] = fmt.Sprintf("%v=%v", name, value)
	}
	return vars
}

func (c *Config) String() string {
	buf, _ := json.Marshal(c)
	buf = pretty.Pretty(buf)
	return string(buf)
}
