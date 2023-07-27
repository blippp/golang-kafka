package main

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/binary"
	"encoding/hex"
	"encoding/pem"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	kafka "github.com/segmentio/kafka-go"
	_ "github.com/segmentio/kafka-go/gzip"
	"golang.org/x/crypto/pkcs12"
)

func main() {
	if !(len(os.Args) == 2) {
		panic("error! an additional parameter must be passed to the input of the program - port number.")
	}

	portNumber := os.Args[1]

	pfx, err := ioutil.ReadFile("c:\\certificates\\asa\\key.pfx")
	if err != nil {
		fmt.Println(err)
		return
	}

	blocks, err := pkcs12.ToPEM(pfx, "Ab123456")
	if err != nil {
		fmt.Println(err)
		return
	}

	var pemData []byte
	for _, b := range blocks {
		pemData = append(pemData, pem.EncodeToMemory(b)...)
	}

	//fmt.Printf("%s\n", pemData)

	cert, err := tls.X509KeyPair(pemData, pemData)
	if err != nil {
		fmt.Println(err)
		return
	}

	//certpool := x509.NewCertPool()
	//capem, err := ioutil.ReadFile("c:\\certificates\\asa\\key.cer")
	//if err != nil {
	//	log.Fatalf("Failed to read client certificate authority: %v", err)
	//}
	//if !certpool.AppendCertsFromPEM(capem) {
	//	log.Fatalf("Can't parse client certificate authority")
	//}

	cfg := &tls.Config{
		Certificates: []tls.Certificate{cert},
		//ClientCAs:    certpool,
	}

	http.HandleFunc("/", handler4Message)

	srv := &http.Server{
		Addr:         "localhost:" + portNumber,
		TLSConfig:    cfg,
		ReadTimeout:  3 * time.Minute,
		WriteTimeout: 3 * time.Minute}

	log.Fatal(srv.ListenAndServeTLS("", ""))
}

func handler4Message(w http.ResponseWriter, r *http.Request) {
	defer func() {
		if x := recover(); x != nil {
			log.Printf("panic: %v", x)
		}
	}()

	portNumberH := os.Args[1]

	if r.Method == "POST" {
		r.ParseForm()

		operation := r.Form.Get("Operation")
		showLogs := r.Form.Get("ShowMsg")
		brokersString := r.Form.Get("BrokersString")
		topic := r.Form.Get("Topic")
		isSecureConn := r.Form.Get("IsSecureConn")
		delayMs, _ := strconv.Atoi(r.Form.Get("DelayMs"))
		cert1 := r.Form.Get("Cert1")
		cert2 := r.Form.Get("Cert2")
		cert3 := r.Form.Get("Cert3")

		var dialer *kafka.Dialer

		if isSecureConn == "n" {
			dialer = &kafka.Dialer{
				Timeout:   time.Duration(delayMs) * time.Millisecond,
				DualStack: true,
			}
		} else {
			tlsConfig := tlsConfig(cert1, cert2, cert3)

			if showLogs == "yes" {
				fmt.Println("!!!tls!!!")
			}

			dialer = &kafka.Dialer{
				Timeout:   time.Duration(delayMs) * time.Millisecond,
				DualStack: true,
				TLS:       tlsConfig,
			}
		}

		////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

		if operation == "Put" {
			brokers := strings.Split(brokersString, ",")
			msgData := r.Form.Get("MsgData")
			msgBatch := strings.Split(msgData, "###")

			config := kafka.WriterConfig{
				Brokers:     brokers,
				Topic:       topic,
				MaxAttempts: 33,
				//BatchTimeout: 30 * time.Millisecond,
				//Balancer:     &kafka.Hash{}, ?????????????????????????????????????
				Dialer: dialer,
			}

			println("=== kafka " + operation + " " + portNumberH + " start ====================================================================================")
			println(time.Now().String())

			if showLogs == "yes" {
				println("msgData -", msgData)
			}

			writer := kafka.NewWriter(config)

			if showLogs == "yes" {
				fmt.Println("Producer configuration:", config)
			}

			defer func() {
				err := writer.Close()
				if err != nil {
					fmt.Println("Error closing producer:", err)
					return
				}
				//fmt.Println("Producer closed")
			}()

			errorMessage := ""

			for i := range msgBatch {
				if strings.Contains(msgBatch[i], "^^^") == true {
					keyMes := strings.Split(msgBatch[i], "^^^")
					key := keyMes[0]
					message := keyMes[1]
					headersString := keyMes[2]

					var headers []kafka.Header
					var header kafka.Header
					if strings.Contains(headersString, "***") == true {
						strHeaders := strings.Split(headersString, "***")
						for j := range strHeaders {
							hkeyMes := strings.Split(strHeaders[j], ":::")

							if hkeyMes[0] == "GPB_MESSAGE_ID" || hkeyMes[0] == "GPB_CHAIN_RQ_ID" {
								headerValue, _ := hex.DecodeString(hkeyMes[1])
								header = kafka.Header{Key: hkeyMes[0], Value: []byte(headerValue)}
							} else {
								if hkeyMes[0] == "GPB_CHAIN_AWAIT_TIMEOUT_MS" {
									gpbChainAwaitTimeout, _ := strconv.ParseInt(hkeyMes[1], 10, 64)
									buf := new(bytes.Buffer)
									binary.Write(buf, binary.BigEndian, gpbChainAwaitTimeout)
									header = kafka.Header{Key: hkeyMes[0], Value: buf.Bytes()}
								} else {
									header = kafka.Header{Key: hkeyMes[0], Value: []byte(hkeyMes[1])}
								}
							}

							headers = append(headers, header)
						}
					}

					var err error
					if len(headers) > 0 {
						err = writer.WriteMessages(context.Background(), kafka.Message{Key: []byte(key), Value: []byte(message), Headers: headers})
					} else {
						err = writer.WriteMessages(context.Background(), kafka.Message{Key: []byte(key), Value: []byte(message)})
					}

					if err == nil {
						fmt.Println("Sent message:", key)
					} else {
						fmt.Println("Error sending message "+key+":", err)
						errorMessage = errorMessage + " Error sending message " + key + ":" + fmt.Sprintf("%v", err)
					}

					//time.Sleep(time.Duration(delayMs) * time.Millisecond)
				}
			}

			if errorMessage != "" {
				println("Error -", errorMessage)
				fmt.Fprint(w, "Error -"+errorMessage)
			} else {
				fmt.Fprint(w, "ok")
			}
		}

		///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

		if operation == "Get" {
			broker := strings.Split(brokersString, ",")[0]
			messCount, _ := strconv.Atoi(r.Form.Get("MessCount"))
			currOffsetsStr := r.Form.Get("CurrOffsetsStr")

			type currOffsetVal struct {
				offset int64
				key    string
			}

			currentOffsets := make(map[int]currOffsetVal)

			for _, currOffsetStr := range strings.Split(currOffsetsStr, "#") {
				if currOffsetStr != "" {
					currOffset := strings.Split(currOffsetStr, ",")
					partition, _ := strconv.Atoi(currOffset[0])
					offset, _ := strconv.ParseInt(currOffset[1], 10, 64)
					currentOffsets[partition] = currOffsetVal{offset, currOffset[2]}
				}
			}

			println("=== kafka " + operation + " " + portNumberH + " start ====================================================================================")
			println(time.Now().String())

			conn, err := dialer.DialLeader(context.Background(), "tcp", broker, topic, 0)
			defer conn.Close()

			if err != nil {
				fmt.Println("DialLeader error:", err)
				return
			}

			partitions, err := conn.ReadPartitions(topic)

			if err != nil {
				fmt.Println("ReadPartitions error:", err)
				return
			}

			messages := ""

			for _, partition := range partitions {
				if showLogs == "yes" {
					fmt.Println("partitionID:", partition.ID)
				}

				offset := currentOffsets[partition.ID].offset

				if offset != 0 {
					connPP, err := dialer.DialLeader(context.Background(), "tcp", broker, topic, partition.ID)
					defer connPP.Close()

					if err != nil {
						fmt.Printf("connPP DialLeader (partition - %d) error:%s\n", partition.ID, err)
						continue
					}

					_, err = connPP.Seek(offset, kafka.SeekAbsolute)
					if err != nil {
						if showLogs == "yes" {
							fmt.Printf("connPP Seek (partition - %d, offset - %d) error:%s\n", partition.ID, offset, err)
						}

						_, err = connPP.Seek(0, kafka.SeekStart)
					}

					connPP.SetReadDeadline(time.Now().Add(time.Duration(delayMs) * time.Millisecond))
					batch := connPP.ReadBatch(0, 1000000000)
					defer batch.Close()

					msg, _ := batch.ReadMessage()

					if string(msg.Key) == currentOffsets[partition.ID].key {
						offset = offset + 1
					} else {
						offset = 0
					}
				}

				connP, err := dialer.DialLeader(context.Background(), "tcp", broker, topic, partition.ID)
				defer connP.Close()

				if err != nil {
					fmt.Printf("connP DialLeader (partition - %d) error:%s\n", partition.ID, err)
					continue
				}

				_, err = connP.Seek(offset, kafka.SeekAbsolute)
				if err != nil {
					if showLogs == "yes" {
						fmt.Printf("connP Seek (partition - %d, offset - %d) error:%s\n", partition.ID, offset, err)
					}

					_, err = connP.Seek(0, kafka.SeekStart)
				}

				connP.SetReadDeadline(time.Now().Add(time.Duration(delayMs) * time.Millisecond))
				batch := connP.ReadBatch(0, 1000000000)
				defer batch.Close()

				for i := 1; i <= messCount; i++ {
					message, err := batch.ReadMessage()

					if err != nil {
						if err == io.EOF {
							err = nil
						}
						//else {
						//	fmt.Printf("connP ReadMessage (partition - %d) error:%s\n", partition.ID, err)
						//}
						break
					}

					headersString := ""

					for i := range message.Headers {
						if message.Headers[i].Key == "GPB_MESSAGE_ID" {
							headersString = headersString + "<GPB_MESSAGE_ID>" + hex.EncodeToString(message.Headers[i].Value) + "</GPB_MESSAGE_ID>"
						}

						if message.Headers[i].Key == "GPB_CORRELATION_ID" {
							headersString = headersString + "<GPB_CORRELATION_ID>" + hex.EncodeToString(message.Headers[i].Value) + "</GPB_CORRELATION_ID>"
						}

						if message.Headers[i].Key == "GPB_PROCESS_RESULT" {
							headersString = headersString + "<GPB_PROCESS_RESULT>" + hex.EncodeToString(message.Headers[i].Value) + "</GPB_PROCESS_RESULT>"
						}

						if message.Headers[i].Key == "GPB_ERROR_DESCRIPTION" {
							headersString = headersString + "<GPB_ERROR_DESCRIPTION>" + string(message.Headers[i].Value) + "</GPB_ERROR_DESCRIPTION>"
						}
					}

					if showLogs == "yes" {
						fmt.Printf("{###}<topic>%s</topic><partition>%d</partition><offset>%d</offset><key>%s</key><message>%s</message>%s\n",
							message.Topic, message.Partition, message.Offset, string(message.Key), string(message.Value), headersString)
					}

					messages = messages + "{###}<topic>" + message.Topic + "</topic><partition>" + strconv.Itoa(message.Partition) + "</partition><offset>" +
						strconv.FormatInt(int64(message.Offset), 10) + "</offset><key>" + string(message.Key) + "</key><message>" + string(message.Value) + "</message>" + headersString
				}
			}

			fmt.Fprintf(w, messages)
		}

		println(time.Now().String())
		println("=== kafka " + operation + " " + portNumberH + " finish =====================================================================================")
	}
}

func tlsConfig(cert1, cert2, cert3 string) *tls.Config {
	//certPEM, err := ioutil.ReadFile("c:\\certificates\\cert4teststand\\public.crt")
	//if err != nil {
	//	fmt.Println(err)
	//} //else {
	////	fmt.Printf("certPEM:%s\n", certPEM)
	////}

	//keyPEM, err := ioutil.ReadFile("c:\\certificates\\cert4teststand\\private.crt")
	//if err != nil {
	//	fmt.Println(err)
	//} //else {
	////	fmt.Printf("keyPEM:%s\n", keyPEM)
	////}

	//caPEM, err := ioutil.ReadFile("c:\\certificates\\cert4teststand\\caroot.crt")
	//if err != nil {
	//	fmt.Println(err)
	//} //else {
	////	fmt.Printf("caPEM:%s\n", caPEM)
	////}

	//certificate, err := tls.X509KeyPair([]byte(certPEM), []byte(keyPEM))
	certificate, err := tls.X509KeyPair([]byte(cert2), []byte(cert1))
	if err != nil {
		fmt.Println(err)
	}

	caCertPool := x509.NewCertPool()
	//if ok := caCertPool.AppendCertsFromPEM([]byte(caPEM)); !ok {
	if ok := caCertPool.AppendCertsFromPEM([]byte(cert3)); !ok {
		fmt.Println(err)
	}

	return &tls.Config{
		Certificates:       []tls.Certificate{certificate},
		RootCAs:            caCertPool,
		Renegotiation:      tls.RenegotiateOnceAsClient,
		MaxVersion:         tls.VersionTLS12,
		InsecureSkipVerify: true,
	}
}
