package crypto

import "golang.org/x/crypto/bcrypt"

func HashPassword(password string) ([]byte, error) {
	hashPassword, err := bcrypt.GenerateFromPassword([]byte(password), bcrypt.DefaultCost)
	if err != nil {
		return nil, err
	}
	return hashPassword, nil
}

func ComparePassword(password string, hashPassword string) error {
	err := bcrypt.CompareHashAndPassword([]byte(hashPassword), []byte(password))
	if err != nil {
		return err
	}
	return nil
}
