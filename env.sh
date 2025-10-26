export DOCKER_HOST=unix:///Users/zoravur/.colima/default/docker.sock                
export TESTCONTAINERS_DOCKER_SOCKET_OVERRIDE=/var/run/docker.sock
export CGO_CFLAGS="-DHAVE_STRCHRNUL -mmacosx-version-min=15.4" 
export MACOSX_DEPLOYMENT_TARGET="15.4"