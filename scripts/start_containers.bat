pushd ..
docker build -t streaming_simulation . -f ./services/streaming_simulation/Dockerfile
docker build -t streaming_processing . -f ./services/streaming_processing/Dockerfile
docker build -t batch_processing . -f ./services/batch_processing/Dockerfile
docker-compose up --remove-orphans