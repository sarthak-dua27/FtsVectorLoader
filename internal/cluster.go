package internal

import (
	"context"
	"fmt"
	"github.com/couchbase/gocb/v2"
	"log"
)

func Initialise_cluster(ctx *context.Context, cluster **gocb.Cluster) {
	var er error
	if capella {
		options := gocb.ClusterOptions{
			Authenticator: gocb.PasswordAuthenticator{
				Username: username,
				Password: password,
			},
			SecurityConfig: gocb.SecurityConfig{
				TLSSkipVerify: true,
			},
		}
		if err := options.ApplyProfile(gocb.
			ClusterConfigProfileWanDevelopment); err != nil {
			log.Fatal(err)
		}
		*cluster, er = gocb.Connect(nodeAddress, options)
	} else {
		*cluster, er = gocb.Connect("couchbase://"+nodeAddress, gocb.ClusterOptions{
			Authenticator: gocb.PasswordAuthenticator{
				Username: username,
				Password: password,
			},
		})
	}
	if er != nil {
		panic(fmt.Errorf("error creating cluster object : %v", er))
	}
}
