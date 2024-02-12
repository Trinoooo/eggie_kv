package logs

import (
	"github.com/Trinoooo/eggie_kv/consts"
	"github.com/Trinoooo/eggie_kv/storage/logs"
	"go.uber.org/zap"
)

var commonFields = []zap.Field{
	zap.String(consts.Core, consts.Ragdoll),
}

var ragdollLogger *zap.Logger

func init() {
	ragdollLogger = logs.Logger.With(commonFields...)
}

func Info(msg string, fields ...zap.Field) {
	ragdollLogger.Info(msg, fields...)
}

func Warn(msg string, fields ...zap.Field) {
	ragdollLogger.Warn(msg, fields...)
}

func Error(msg string, fields ...zap.Field) {
	ragdollLogger.Error(msg, fields...)
}

func Fatal(msg string, fields ...zap.Field) {
	ragdollLogger.Fatal(msg, fields...)
}
