package transport

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"math/big"
	"net"
	"time"
)

// DefaultALPN is the standard ALPN identifier for MQTT over QUIC.
const DefaultALPN = "mqtt"

// DefaultClientSessionCacheCapacity is the default capacity for the TLS session ticket cache.
const DefaultClientSessionCacheCapacity = 64

// NewClientTLSConfig creates a TLS 1.3 client configuration with ALPN and session cache for 0-RTT.
func NewClientTLSConfig(insecureSkipVerify bool, serverName string, alpn ...string) *tls.Config {
	alpns := alpn
	if len(alpns) == 0 {
		alpns = []string{DefaultALPN}
	}

	return &tls.Config{
		ServerName:         serverName,
		InsecureSkipVerify: insecureSkipVerify,
		NextProtos:         alpns,
		MinVersion:         tls.VersionTLS13,
		ClientSessionCache: tls.NewLRUClientSessionCache(DefaultClientSessionCacheCapacity),
	}
}

// NewServerTLSConfig creates a TLS 1.3 server configuration for the given certificate and ALPN.
func NewServerTLSConfig(cert tls.Certificate, alpn ...string) *tls.Config {
	alpns := alpn
	if len(alpns) == 0 {
		alpns = []string{DefaultALPN}
	}

	return &tls.Config{
		Certificates: []tls.Certificate{cert},
		NextProtos:   alpns,
		MinVersion:   tls.VersionTLS13,
	}
}

// GenerateSelfSignedCertificate generates a self-signed TLS certificate for local dev and testing.
func GenerateSelfSignedCertificate(hosts ...string) (tls.Certificate, error) {
	priv, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return tls.Certificate{}, err
	}

	notBefore := time.Now().Add(-1 * time.Hour)
	notAfter := notBefore.Add(24 * time.Hour * 365)

	serialNumberLimit := new(big.Int).Lsh(big.NewInt(1), 128)
	serialNumber, err := rand.Int(rand.Reader, serialNumberLimit)
	if err != nil {
		return tls.Certificate{}, err
	}

	template := x509.Certificate{
		SerialNumber: serialNumber,
		Subject: pkix.Name{
			Organization: []string{"mqttOQ Development"},
			CommonName:   "mqttOQ Localhost",
		},
		NotBefore:             notBefore,
		NotAfter:              notAfter,
		KeyUsage:              x509.KeyUsageKeyEncipherment | x509.KeyUsageDigitalSignature,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth},
		BasicConstraintsValid: true,
	}

	if len(hosts) == 0 {
		hosts = []string{"127.0.0.1", "localhost"}
	}

	for _, h := range hosts {
		if ip := net.ParseIP(h); ip != nil {
			template.IPAddresses = append(template.IPAddresses, ip)
		} else {
			template.DNSNames = append(template.DNSNames, h)
		}
	}

	derBytes, err := x509.CreateCertificate(rand.Reader, &template, &template, &priv.PublicKey, priv)
	if err != nil {
		return tls.Certificate{}, err
	}

	cert := tls.Certificate{
		Certificate: [][]byte{derBytes},
		PrivateKey:  priv,
	}

	return cert, nil
}
