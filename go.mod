module github.com/go-ap/fedbox

go 1.14

require (
	github.com/dgraph-io/badger/v2 v2.0.3
	github.com/dgraph-io/ristretto v0.0.2 // indirect
	github.com/dgryski/go-farm v0.0.0-20200201041132-a6ae2369ad13 // indirect
	github.com/gchaincl/dotsql v1.0.0
	github.com/go-ap/activitypub v0.0.0-20200419090215-7e300c1773e7
	github.com/go-ap/auth v0.0.0-20200419090942-8997c9899555
	github.com/go-ap/client v0.0.0-20200419090630-d342b427b2a1
	github.com/go-ap/errors v0.0.0-20200402124111-0e465c0b25bc
	github.com/go-ap/handlers v0.0.0-20200419092018-be21672553e9
	github.com/go-ap/jsonld v0.0.0-20200327122108-fafac2de2660
	github.com/go-ap/processing v0.0.0-20200419092153-e5ca80e54adb
	github.com/go-ap/storage v0.0.0-20200419090742-6ac09811d312
	github.com/go-chi/chi v4.1.1+incompatible
	github.com/golang/protobuf v1.3.4 // indirect
	github.com/jackc/pgx v3.6.2+incompatible
	github.com/joho/godotenv v1.3.0
	github.com/kr/pretty v0.2.0 // indirect
	github.com/mariusor/qstring v0.0.0-20200204164351-5a99d46de39d
	github.com/openshift/osin v1.0.1
	github.com/pborman/uuid v1.2.0
	github.com/sirupsen/logrus v1.5.0
	github.com/spacemonkeygo/httpsig v0.0.0-20181218213338-2605ae379e47
	github.com/unrolled/render v1.0.2
	go.etcd.io/bbolt v1.3.4
	golang.org/x/crypto v0.0.0-20200414173820-0848c9571904
	golang.org/x/net v0.0.0-20200301022130-244492dfa37a // indirect
	golang.org/x/oauth2 v0.0.0-20200107190931-bf48bf16ab8d
	google.golang.org/appengine v1.6.5 // indirect
	gopkg.in/urfave/cli.v2 v2.0.0-20190806201727-b62605953717
)

replace (
	github.com/go-ap/activitypub => /home/habarnam/workspace/go-ap/activitypub
	github.com/go-ap/auth => /home/habarnam/workspace/go-ap/auth
	github.com/go-ap/errors => /home/habarnam/workspace/go-ap/errors
	github.com/go-ap/jsonld => /home/habarnam/workspace/go-ap/jsonld
	github.com/go-ap/processing => /home/habarnam/workspace/go-ap/processing
	github.com/go-ap/storage => /home/habarnam/workspace/go-ap/storage
)
