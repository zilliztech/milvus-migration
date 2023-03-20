package main

import (
	_ "github.com/zilliztech/milvus-migration/asap"
	"github.com/zilliztech/milvus-migration/cmd"
)

// @title           Migration Swagger API
// @version         1.0

// @license.name  Apache 2.0
// @license.url   http://www.apache.org/licenses/LICENSE-2.0.html

// @BasePath  /api/v1
func main() {
	cmd.Execute()
}
