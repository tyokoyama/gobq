package gobq

import (
	"fmt"
	"io/ioutil"
	"net/http"

	"code.google.com/p/goauth2/oauth"
	"code.google.com/p/goauth2/oauth/jwt"
)

const (
	cachefile = "cache.json"
)

type ClientConfig struct {
	ClientId string
	ClientSecret string
	RedirectUri string
	Scope string
	RequestTokenUrl string
	AuthTokenUrl string
}

func NewServiceAccountClient(email, scope, secretFileName string) (*http.Client, error) {
	// goauth2の認証（Service Account認証）
	gKey, err := ioutil.ReadFile(secretFileName)
	if err != nil {
		return nil, err
	}

	token := jwt.NewToken(email, scope, gKey)

	transport, err := jwt.NewTransport(token)
	if err != nil {
		return nil, err
	}

	client := transport.Client()

	return client, nil
}

func NewOAuth2Client(config ClientConfig, code string) (*http.Client, error) {

	oauthconfig := &oauth.Config{
            ClientId:     config.ClientId,
            ClientSecret: config.ClientSecret,
            RedirectURL:  config.RedirectUri,
            Scope:        config.Scope,
            AuthURL:      config.RequestTokenUrl,
            TokenURL:     config.AuthTokenUrl,
            TokenCache:   oauth.CacheFile(cachefile),
    }

    transport := &oauth.Transport{Config: oauthconfig}

    // キャッシュからトークンファイルを取得
    _, err := oauthconfig.TokenCache.Token()
    if err != nil {
        // キャッシュなし

        // 認証コードなし＝＞ブラウザで認証させるためにURLを出力
        if code == "" {
            url := oauthconfig.AuthCodeURL("")
            fmt.Println("Please Authorize. this URL.")
            fmt.Println(url)
            return nil, err
        }

        // 認証トークンを取得する。（取得後、キャッシュへ）
        _, err = transport.Exchange(code)
        if err != nil {
            fmt.Println("Exchange: ", err)
            return nil, err
        }

    }

    return transport.Client(), nil
}