from prefect. deployments import Deployment
from prefect.infrastructure.docker import DockerContainer
from parametrized_flow import etl_parent_flow

docker_block = DockerContainer.load("docker-new")

docker_dep = Deployment.build_from_flow(
    flow=etl_parent_flow, 
    name= 'docker-flowing',
    infrastructure=docker_block
)


if __name__ == '__main__':
    docker_dep.apply()