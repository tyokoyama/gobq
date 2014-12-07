# Bigquery APIをGoで楽に実行するためのライブラリ

## Version
0.0.1

## 使い方
テストコードを参照して下さい。

## 注意事項
1. ただのWrapperなので、事前にgoauth2とgoogle-api-go-clientをgo getしておいて下さい。
	1. code.google.com/p/goauth2/oauth
	1. go get code.google.com/p/goauth2/oauth/jwt
1. go get code.google.com/p/google-api-go-client/bigquery/v2
1. テストデータは公開されているデータを利用していますが、再配布が可能かどうかがわからないのでひとまずリポジトリに入れていません。
1. 実行した後、**何が起こっても怒らないで下さい**。
1. Pull Requestはいつでもどうぞ。
1. 著作権はT.Yokoyamaにありますが、MIT Licenseにしてあります。
