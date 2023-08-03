package s3

import (
	"bytes"
	"context"
	"io/ioutil"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/aws/signer/v4"
	"github.com/aws/aws-sdk-go-v2/credentials"
	s3sdk "github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/magiconair/properties"
	"github.com/pingcap/go-ycsb/pkg/prop"
	"github.com/pingcap/go-ycsb/pkg/util"
	"github.com/pingcap/go-ycsb/pkg/ycsb"
)

const (
	s3AccessKey    = "s3.access_key"
	s3SecretKey    = "s3.secret_key"
	s3Endpoint     = "s3.endpoint"
	s3Region       = "s3.region"
	s3UsePathStyle = "s3.use_pathstyle"
	s3SkipChecksum = "s3.skip_checksum"
)

type s3Creator struct{}

func (c s3Creator) Create(p *properties.Properties) (ycsb.DB, error) {
	accessKey := p.GetString(s3AccessKey, "")
	secretKey := p.GetString(s3SecretKey, "")
	endpoint := p.GetString(s3Endpoint, "")
	region := p.GetString(s3Region, "us-east-1")
	usePathStyle := p.GetBool(s3UsePathStyle, true)
	skipChecksum := p.GetBool(s3SkipChecksum, true)

	var resolver s3sdk.EndpointResolver
	resolver = s3sdk.NewDefaultEndpointResolver()
	if endpoint != "" {
		resolver = s3sdk.EndpointResolverFromURL(endpoint)
	}

	var optFns []func(*s3sdk.Options)
	optFns = append(optFns, s3sdk.WithEndpointResolver(resolver))
	if skipChecksum {
		optFns = append(optFns, s3sdk.WithAPIOptions(v4.SwapComputePayloadSHA256ForUnsignedPayloadMiddleware))
	}

	client := s3sdk.New(
		s3sdk.Options{
			UsePathStyle: usePathStyle,
			Credentials:  credentials.NewStaticCredentialsProvider(accessKey, secretKey, ""),
			Region:       region,
		}, optFns...)

	return &s3DB{
		db:         client,
		codec:      util.NewRowCodec(p),
		bufPool:    util.NewBufPool(),
		fieldCount: p.GetInt64(prop.FieldCount, prop.FieldCountDefault),
	}, nil
}

type s3DB struct {
	db         *s3sdk.Client
	codec      *util.RowCodec
	bufPool    *util.BufPool
	fieldCount int64
}

func (db *s3DB) Close() error {
	return nil
}

func (db *s3DB) InitThread(ctx context.Context, _ int, _ int) context.Context {
	return ctx
}

func (db *s3DB) CleanupThread(_ context.Context) {
}

func (db *s3DB) Read(ctx context.Context, table string, key string, fields []string) (map[string][]byte, error) {
	output, err := db.db.GetObject(ctx, &s3sdk.GetObjectInput{
		Bucket: aws.String(table),
		Key:    aws.String(key),
	})
	if err != nil {
		return nil, err
	}

	defer output.Body.Close()
	value, err := ioutil.ReadAll(output.Body)
	if err != nil {
		return nil, err
	}
	return db.codec.Decode(value, fields)
}

func (db *s3DB) Scan(ctx context.Context, table string, startKey string, count int, fields []string) ([]map[string][]byte, error) {
	res := make([]map[string][]byte, 0, count)

	var contToken *string
	left := count
	for left > 0 {
		startAfter := before(startKey)
		maxKeys := 1000
		if left < maxKeys {
			maxKeys = left
		}
		output, err := db.db.ListObjectsV2(ctx, &s3sdk.ListObjectsV2Input{
			Bucket:            aws.String(table),
			StartAfter:        aws.String(startAfter),
			ContinuationToken: contToken,
			MaxKeys:           int32(maxKeys),
		})
		if err != nil {
			return nil, err
		}

		for _, o := range output.Contents {
			res = append(res, map[string][]byte{*o.Key: nil})
		}

		contToken = output.NextContinuationToken
		left -= int(output.KeyCount)

		if !output.IsTruncated {
			break
		}
	}

	return res, nil
}

func before(key string) string {
	if len(key) <= 0 {
		return ""
	}
	runes := []rune(key)
	runes[len(runes)-1]--
	return string(runes)
}

func (db *s3DB) Update(ctx context.Context, table string, key string, values map[string][]byte) error {
	// full update
	if int64(len(values)) == db.fieldCount {
		return db.Insert(ctx, table, key, values)
	}

	// partial update
	m, err := db.Read(ctx, table, key, nil)
	if err != nil {
		return err
	}

	for field, value := range values {
		m[field] = value
	}

	buf := db.bufPool.Get()
	defer db.bufPool.Put(buf)

	buf, err = db.codec.Encode(buf, m)
	if err != nil {
		return err
	}

	_, err = db.db.PutObject(ctx, &s3sdk.PutObjectInput{
		Bucket:        aws.String(table),
		Key:           aws.String(key),
		Body:          bytes.NewReader(buf),
		ContentLength: int64(len(buf)),
	})
	return err
}

func (db *s3DB) Insert(ctx context.Context, table string, key string, values map[string][]byte) error {
	buf := db.bufPool.Get()
	defer db.bufPool.Put(buf)

	buf, err := db.codec.Encode(buf, values)
	if err != nil {
		return err
	}

	_, err = db.db.PutObject(ctx, &s3sdk.PutObjectInput{
		Bucket:        aws.String(table),
		Key:           aws.String(key),
		Body:          bytes.NewReader(buf),
		ContentLength: int64(len(buf)),
	})
	return err
}

func (db *s3DB) Delete(ctx context.Context, table string, key string) error {
	_, err := db.db.DeleteObject(ctx, &s3sdk.DeleteObjectInput{
		Bucket: aws.String(table),
		Key:    aws.String(key),
	})
	return err
}

func init() {
	ycsb.RegisterDBCreator("s3", s3Creator{})
}
