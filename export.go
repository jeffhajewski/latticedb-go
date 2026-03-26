package latticedb

import "github.com/jeffhajewski/latticedb-go/internal/exporter"

type ExportFormat string

const (
	ExportFormatJSON  ExportFormat = "json"
	ExportFormatJSONL ExportFormat = "jsonl"
	ExportFormatCSV   ExportFormat = "csv"
	ExportFormatDOT   ExportFormat = "dot"
)

func Export(dbPath string, format ExportFormat, outputPath string) ([]byte, error) {
	return exporter.Export(dbPath, exporter.ExportFormat(format), outputPath)
}

func Dump(dbPath string) ([]byte, error) {
	return exporter.Dump(dbPath)
}

func SimulateCrash(dbPath string) error {
	return exporter.SimulateCrash(dbPath)
}
