package sliceutil

func ChunkWithSize[T any](slice []T, chunkSize int) [][]T {
	n := (len(slice) + chunkSize - 1) / chunkSize
	chunks := make([][]T, 0, n)
	for i := 0; i < len(slice); i += chunkSize {
		end := i + chunkSize
		if end > len(slice) {
			end = len(slice)
		}
		chunks = append(chunks, slice[i:end])
	}
	return chunks
}
