// +build !go1.16

package providertest

import (
	"io/ioutil"
	"os"
)

// cp - copies file
func cp(src, dst string) error {
	if _, err := os.Stat(src); err != nil {
		return err
	}

	in, err := ioutil.ReadFile(src)
	if err != nil {
		return err
	}
	if err := ioutil.WriteFile(dst, in, 0644); err != nil {
		return err
	}
	return nil
}
