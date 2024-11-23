pushd ..
docker build -t streaming_simulation . -f ./services/streaming_simulation/Dockerfile
docker-compose up