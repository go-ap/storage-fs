package fs

import (
	"errors"
	"os"
	"reflect"
	"testing"
)

func Test_New(t *testing.T) {
	testFolder := t.TempDir()
	cwd, _ := os.Getwd()

	tests := []struct {
		name   string
		config Config
		want   *repo
		err    error
	}{
		{
			name:   "empty",
			config: Config{},
			want:   nil,
			err:    errMissingPath,
		},
		{
			name:   "invalid permissions",
			config: Config{Path: "/root/tmp"},
			want:   nil,
			err:    os.ErrPermission,
		},
		{
			name:   "invalid relative file",
			config: Config{Path: "./not-sure-if-this-should-work-or-not"},
			want:   nil,
			err:    os.ErrNotExist,
		},
		{
			name:   "valid temp folder",
			config: Config{Path: testFolder},
			want: &repo{
				path: testFolder,
				cwd:  cwd,
			},
			err: nil,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := New(tt.config)
			if !errors.Is(err, tt.err) {
				t.Errorf("New() returned different error than expected = %v, want %v", err, tt.err)
			}
			if got != nil {
				// these make the deep equal fail
				got.cache = nil
				got.logFn = nil
				got.errFn = nil
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("New() = %v, want %v", got, tt.want)
			}
		})
	}
}
