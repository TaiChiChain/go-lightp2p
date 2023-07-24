package cert

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/sha256"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"io/ioutil"
	"math/big"
	"path/filepath"
	"time"

	"github.com/meshplus/bitxhub-kit/crypto"
	ecdsa2 "github.com/meshplus/bitxhub-kit/crypto/asym/ecdsa"
)

type Certs struct {
	NodeCertData   []byte
	AgencyCertData []byte
	CACertData     []byte
	NodeCert       *x509.Certificate
	AgencyCert     *x509.Certificate
	CACert         *x509.Certificate
}

func LoadCerts(repoRoot, nodeCertPath, agencyCertPath, caCertPath string) (*Certs, error) {
	nodeCert, nodeCertData, err := loadCert(filepath.Join(repoRoot, nodeCertPath))
	if err != nil {
		return nil, fmt.Errorf("load node certs: %w", err)
	}

	agencyCert, agencyCertData, err := loadCert(filepath.Join(repoRoot, agencyCertPath))
	if err != nil {
		return nil, fmt.Errorf("load agency certs: %w", err)
	}
	caCert, caCertData, err := loadCert(filepath.Join(repoRoot, caCertPath))
	if err != nil {
		return nil, fmt.Errorf("load ca certs: %w", err)
	}

	return &Certs{
		NodeCertData:   nodeCertData,
		AgencyCertData: agencyCertData,
		CACertData:     caCertData,
		NodeCert:       nodeCert,
		AgencyCert:     agencyCert,
		CACert:         caCert,
	}, nil
}

func loadCert(certPath string) (*x509.Certificate, []byte, error) {
	data, err := ioutil.ReadFile(certPath)
	if err != nil {
		return nil, nil, fmt.Errorf("read certs: %w", err)
	}

	cert, err := ParseCert(data)
	if err != nil {
		return nil, nil, fmt.Errorf("parse certs: %w", err)
	}

	return cert, data, nil
}

func VerifySign(subCert *x509.Certificate, caCert *x509.Certificate) error {
	if err := subCert.CheckSignatureFrom(caCert); err != nil {
		return fmt.Errorf("check sign: %w", err)
	}

	if subCert.NotBefore.After(time.Now()) || subCert.NotAfter.Before(time.Now()) {
		return fmt.Errorf("certs expired")
	}

	return nil
}

func ParsePrivateKey(data []byte, opt crypto.KeyType) (*ecdsa2.PrivateKey, error) {
	if data == nil {
		return nil, fmt.Errorf("empty data")
	}

	block, _ := pem.Decode(data)
	if block == nil {
		return nil, fmt.Errorf("empty block")
	}

	return ecdsa2.UnmarshalPrivateKey(block.Bytes, opt)
}

func ParseCert(data []byte) (*x509.Certificate, error) {
	if data == nil {
		return nil, fmt.Errorf("empty data")
	}

	block, _ := pem.Decode(data)
	if block == nil {
		return nil, fmt.Errorf("empty block")
	}

	return x509.ParseCertificate(block.Bytes)
}

func GenerateCert(privKey *ecdsa.PrivateKey, isCA bool, organization string) (*x509.Certificate, error) {
	sn, err := rand.Int(rand.Reader, big.NewInt(1000000))
	if err != nil {
		return nil, err
	}
	notBefore := time.Now().Add(-5 * time.Minute).UTC()

	template := &x509.Certificate{
		SerialNumber:          sn,
		NotBefore:             notBefore,
		NotAfter:              notBefore.Add(50 * 365 * 24 * time.Hour).UTC(),
		BasicConstraintsValid: true,
		IsCA:                  isCA,
		KeyUsage: x509.KeyUsageDigitalSignature |
			x509.KeyUsageKeyEncipherment | x509.KeyUsageCertSign |
			x509.KeyUsageCRLSign,
		ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageAny},
		Subject: pkix.Name{
			Country:            []string{"CN"},
			Locality:           []string{"HangZhou"},
			Province:           []string{"ZheJiang"},
			OrganizationalUnit: []string{"BitXHub"},
			Organization:       []string{organization},
			StreetAddress:      []string{"street", "address"},
			PostalCode:         []string{"324000"},
			CommonName:         "bitxhub.cn",
		},
	}
	template.SubjectKeyId = priKeyHash(privKey)

	return template, nil
}

func priKeyHash(priKey *ecdsa.PrivateKey) []byte {
	hash := sha256.New()

	_, err := hash.Write(elliptic.Marshal(priKey.Curve, priKey.PublicKey.X, priKey.PublicKey.Y))
	if err != nil {
		fmt.Printf("Get private key hash: %s", err.Error())
		return nil
	}

	return hash.Sum(nil)
}

type CertsMessage struct {
	AgencyCert []byte
	NodeCert   []byte
}

func (c *CertsMessage) Marshal() ([]byte, error) {
	return json.Marshal(c)
}

func (c *CertsMessage) Unmarshal(data []byte) error {
	return json.Unmarshal(data, c)
}
