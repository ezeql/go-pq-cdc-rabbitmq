package bytesize

import (
	"errors"
	"testing"
)

func TestParseSize(t *testing.T) {
	t.Parallel()

	tests := []struct {
		wantErr error
		name    string
		input   string
		want    Size
	}{
		{name: "kb", input: "1kb", want: KB},
		{name: "mb", input: "1mb", want: MB},
		{name: "gb", input: "2gb", want: 2 * GB},
		{name: "spaces", input: " 10 mb ", want: 10 * MB},
		{name: "invalid number", input: "xmb", wantErr: ErrInvalidNumberFormat},
		{name: "unknown unit", input: "10xb", wantErr: ErrUnknownUnit},
		{name: "no unit", input: "10", wantErr: ErrInvalidSizeFormat},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got, err := ParseSize(tt.input)
			if tt.wantErr != nil {
				if !errors.Is(err, tt.wantErr) {
					t.Fatalf("expected %v, got %v", tt.wantErr, err)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got != tt.want {
				t.Fatalf("expected %d, got %d", tt.want, got)
			}
		})
	}
}
