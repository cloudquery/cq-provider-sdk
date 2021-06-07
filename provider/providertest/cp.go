// +build go1.16

package providertest

import "os"

// cp - copies file
func cp(src, dst string) error {
	if _, err := os.Stat(src); err != nil {
		return err
	}

	in, err := os.ReadFile(src)
	if err != nil {
		return err
	}
	if err := os.WriteFile(dst, in, 0644); err != nil {
		return err
	}
	return nil
}
