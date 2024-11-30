package utils

import "strings"

// URL concatenates a base URL and a path, after removing a trailing slash from
// the base URL (if any) and a leading slash from the path (if any).
//
// TODO: replace all uses of this with the proper path utils
func URL(base, path string) string {
	return strings.TrimSuffix(base, "/") + "/" + strings.TrimPrefix(path, "/")
}
