package fs

import (
	"os"
	"testing"
)

func Test_loadBinFromFile(t *testing.T) {
	type args struct {
		root *os.Root
		path string
		bmp  any
	}
	tests := []struct {
		name    string
		args    args
		wantErr bool
	}{
		{
			name:    "empty",
			args:    args{},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := loadBinFromFile(tt.args.root, tt.args.path, tt.args.bmp); (err != nil) != tt.wantErr {
				t.Errorf("loadBinFromFile() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func Test_writeBinFile(t *testing.T) {
	type args struct {
		root *os.Root
		path string
		bmp  any
	}
	tests := []struct {
		name    string
		args    args
		wantErr bool
	}{
		{
			name:    "empty",
			args:    args{},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := writeBinFile(tt.args.root, tt.args.path, tt.args.bmp); (err != nil) != tt.wantErr {
				t.Errorf("writeBinFile() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}
