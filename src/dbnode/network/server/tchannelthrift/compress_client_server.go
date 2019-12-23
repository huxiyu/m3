// Copyright (c) 2019 Uber Technologies, Inc.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package tchannelthrift

import (
	"sync"

	"github.com/m3db/m3/src/dbnode/generated/thrift/rpc"

	apachethrift "github.com/apache/thrift/lib/go/thrift"
	"github.com/uber/tchannel-go/thrift"
)

var _ thrift.TChanClient = (*snappyTChanClient)(nil)

type snappyTChanClient struct {
	client thrift.TChanClient
}

// NewSnappyTChanClient creates a new snappy TChanClient.
func NewSnappyTChanClient(client thrift.TChanClient) thrift.TChanClient {
	return &snappyTChanClient{client: client}
}

func (c *snappyTChanClient) Call(
	ctx thrift.Context,
	serviceName, methodName string,
	req, resp apachethrift.TStruct,
) (bool, error) {
	snappyReq := snappyTStructPool.Get().(*snappyTStruct)
	snappyReq.thriftStruct = req

	snappyResp := snappyTStructPool.Get().(*snappyTStruct)
	snappyResp.thriftStruct = resp

	result, err := c.client.Call(ctx, serviceName, methodName, snappyReq, snappyResp)

	snappyTStructPool.Put(snappyReq)
	snappyTStructPool.Put(snappyResp)

	return result, err
}

var snappyTStructPool = sync.Pool{
	New: func() interface{} {
		return &snappyTStruct{}
	},
}

var _ apachethrift.TStruct = (*snappyTStruct)(nil)

type snappyTStruct struct {
	thriftStruct apachethrift.TStruct
}

func (s *snappyTStruct) Write(p apachethrift.TProtocol) error {
	var writeMessageEnd bool
	compressibleProtocol, ok := p.(apachethrift.TCompressibleProtocol)
	defer func() {
		// Always make sure to read call read message end even if encounter
		// an error so that cleanup can occur.
		if !writeMessageEnd && ok {
			_ = compressibleProtocol.WriteCompressibleMessageEnd()
		}
	}()

	if ok {
		opts := apachethrift.WriteCompressibleMessageBeginOptions{
			Compress: true,
		}
		err := compressibleProtocol.WriteCompressibleMessageBegin(opts)
		if err != nil {
			return err
		}
	}

	if err := s.thriftStruct.Write(p); err != nil {
		return err
	}

	if ok {
		writeMessageEnd = true
		err := compressibleProtocol.WriteCompressibleMessageEnd()
		if err != nil {
			return err
		}
	}

	return nil
}

func (s *snappyTStruct) Read(p apachethrift.TProtocol) error {
	var readMessageEnd bool
	compressibleProtocol, ok := p.(apachethrift.TCompressibleProtocol)
	defer func() {
		// Always make sure to read call read message end even if encounter
		// an error so that cleanup can occur.
		if !readMessageEnd && ok {
			_, _ = compressibleProtocol.ReadCompressibleMessageEnd()
		}
	}()

	if ok {
		err := compressibleProtocol.ReadCompressibleMessageBegin()
		if err != nil {
			return err
		}
	}

	if err := s.thriftStruct.Read(p); err != nil {
		return err
	}

	if ok {
		readMessageEnd = true
		_, err := compressibleProtocol.ReadCompressibleMessageEnd()
		if err != nil {
			return err
		}
	}

	return nil
}

type snappyTChanNodeServer struct {
	thrift.TChanServer
}

// NewSnappyTChanNodeServer returns a new snappy TChanNodeServer.
func NewSnappyTChanNodeServer(handler rpc.TChanNode) thrift.TChanServer {
	return &snappyTChanNodeServer{
		TChanServer: rpc.NewTChanNodeServer(handler),
	}
}

func (s *snappyTChanNodeServer) Handle(
	ctx thrift.Context,
	methodName string,
	protocol apachethrift.TProtocol,
) (bool, apachethrift.TStruct, error) {
	var readMessageEnd bool
	compressibleProtocol, ok := protocol.(apachethrift.TCompressibleProtocol)
	defer func() {
		// Always make sure to read call read message end even if encounter
		// an error so that cleanup can occur.
		if !readMessageEnd && ok {
			_, _ = compressibleProtocol.ReadCompressibleMessageEnd()
		}
	}()

	if ok {
		err := compressibleProtocol.ReadCompressibleMessageBegin()
		if err != nil {
			return false, nil, err
		}
	}

	result, resp, err := s.TChanServer.Handle(ctx, methodName, protocol)
	if err != nil {
		return result, resp, err
	}

	if ok {
		readMessageEnd = true
		reqReadResult, err := compressibleProtocol.ReadCompressibleMessageEnd()
		if err != nil {
			return false, nil, err
		}
		if result && reqReadResult.Compressed {
			// If the request was compressed and result is successful, then
			// wrap the response in a snappy struct that will compress the
			// response back to the client.
			resp = &snappyTStruct{thriftStruct: resp}
		}
	}

	return result, resp, err
}
