package logutil

import (
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

// Values groups a set of zap.Fields under a single "values" object field.
// Zero reflection, same speed as inline fields.
func Values(fields ...zap.Field) zap.Field {
	return zap.Object("values", zapcore.ObjectMarshalerFunc(func(enc zapcore.ObjectEncoder) error {
		for _, f := range fields {
			f.AddTo(enc)
		}
		return nil
	}))
}
