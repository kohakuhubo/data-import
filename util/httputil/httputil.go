package httputil

import (
	"net/http"
	"strings"
)

const (
	unknownIp = "unknown"
	defaultIp = "0.0.0.0"
)

type Environment struct {
	Platform      string
	PlatformV     string
	PlatformModel string
	PlatformBrand string
	ClientV       string
	DeviceId      string
	NetworkType   string
	Ip            string
}

func GetEnvironment(r *http.Request) *Environment {

	env := new(Environment)

	platforms := r.Header["platform"]
	if platforms != nil && len(platforms) > 0 {
		env.Platform = strings.Join(platforms, ",")
	}

	platformVs := r.Header["platform-v"]
	if platformVs != nil && len(platformVs) > 0 {
		env.PlatformV = strings.Join(platformVs, ",")
	}

	platformModels := r.Header["platform-model"]
	if platformModels != nil && len(platformModels) > 0 {
		env.PlatformModel = strings.Join(platformModels, ",")
	}

	platformBrands := r.Header["platform-brand"]
	if platformBrands != nil && len(platformBrands) > 0 {
		env.PlatformBrand = strings.Join(platformBrands, ",")
	}

	clientVs := r.Header["client-v"]
	if clientVs != nil && len(clientVs) > 0 {
		env.ClientV = strings.Join(clientVs, ",")
	}

	deviceIds := r.Header["device-id"]
	if deviceIds != nil && len(deviceIds) > 0 {
		env.DeviceId = strings.Join(deviceIds, ",")
	}

	networkTypes := r.Header["network-type"]
	if networkTypes != nil && len(networkTypes) > 0 {
		env.NetworkType = strings.Join(networkTypes, ",")
	}

	env.Ip = GetIP(r.Header, r)

	return env
}

func GetIP(header http.Header, r *http.Request) string {

	if header != nil {

		var Xfor string
		if header["X-Forwarded-For"] != nil && len(header["X-Forwarded-For"]) > 0 {
			Xfor = strings.Join(header["X-Forwarded-For"], ",")
		}

		var Xip string
		if header["X-Real-IP"] != nil && len(header["X-Real-IP"]) > 0 {
			Xip = strings.Join(header["X-Real-IP"], ",")
		}

		if Xfor != "" && unknownIp != Xfor {
			return header["X-Forwarded-For"][0]
		}

		Xfor = Xip
		if Xfor != "" && unknownIp != Xfor {
			return Xfor
		}

		if Xfor == "" || unknownIp == Xfor {
			if header["Proxy-Client-IP"] != nil && len(header["Proxy-Client-IP"]) > 0 {
				Xfor = strings.Join(header["Proxy-Client-IP"], ",")
			}
		}

		if Xfor == "" || unknownIp == Xfor {
			if header["WL-Proxy-Client-IP"] != nil && len(header["WL-Proxy-Client-IP"]) > 0 {
				Xfor = strings.Join(header["WL-Proxy-Client-IP"], ",")
			}
		}

		if Xfor == "" || unknownIp == Xfor {
			if header["HTTP_CLIENT_IP"] != nil && len(header["HTTP_CLIENT_IP"]) > 0 {
				Xfor = strings.Join(header["HTTP_CLIENT_IP"], ",")
			}
		}

		if Xfor == "" || unknownIp == Xfor {
			if header["HTTP_X_FORWARDED_FOR"] != nil && len(header["HTTP_X_FORWARDED_FOR"]) > 0 {
				Xfor = strings.Join(header["HTTP_X_FORWARDED_FOR"], ",")
			}
		}

		if Xfor == "" || unknownIp == Xfor {
			Xfor = r.RemoteAddr
		}

		return Xfor

	}

	return defaultIp
}
