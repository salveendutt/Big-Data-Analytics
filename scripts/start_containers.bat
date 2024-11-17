pushd ..
docker build -t streaming_simulation . -f ./streaming_simulation/Dockerfile
docker-compose up