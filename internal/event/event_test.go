package event

import (
	"testing"
)

func TestChunk(t *testing.T) {
	t.Run("empty", func(t *testing.T) {
		if got := Chunk[int](nil, 3); got != nil {
			t.Fatalf("expected nil, got %v", got)
		}
	})
	t.Run("exact multiple", func(t *testing.T) {
		got := Chunk([]int{1, 2, 3, 4}, 2)
		if len(got) != 2 || len(got[0]) != 2 || len(got[1]) != 2 {
			t.Fatalf("unexpected chunks: %v", got)
		}
	})
	t.Run("remainder", func(t *testing.T) {
		got := Chunk([]int{1, 2, 3, 4, 5}, 2)
		if len(got) != 3 || len(got[2]) != 1 {
			t.Fatalf("unexpected chunks: %v", got)
		}
	})
	t.Run("size larger than slice", func(t *testing.T) {
		got := Chunk([]int{1, 2}, 10)
		if len(got) != 1 || len(got[0]) != 2 {
			t.Fatalf("unexpected chunks: %v", got)
		}
	})
}
