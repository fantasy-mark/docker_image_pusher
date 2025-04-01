import gzip
import hashlib
import json
import os
import shutil
import sys
import tarfile
from typing import Dict, List, Optional, Tuple

import requests
import urllib3

# Disable SSL warnings
urllib3.disable_warnings()


class DockerImagePuller:
    def __init__(self, image_reference: str):
        self.image_reference = image_reference
        self.registry = 'registry-1.docker.io'
        self.repo = 'library'
        self.tag = 'latest'
        self.img = ''
        self.auth_url = 'https://auth.docker.io/token'
        self.reg_service = 'registry.docker.io'
        self.image_dir = ''

    def parse_image_reference(self) -> None:
        """Parse the image reference into registry, repository, image and tag components."""
        parts = self.image_reference.split('/')
        
        # Extract image and tag/digest
        last_part = parts[-1]
        if '@' in last_part:
            self.img, self.tag = last_part.split('@')
        elif ':' in last_part:
            self.img, self.tag = last_part.split(':')
        else:
            self.img = last_part

        # Determine registry and repository
        if len(parts) > 1 and ('.' in parts[0] or ':' in parts[0]):
            self.registry = parts[0]
            self.repo = '/'.join(parts[1:-1])
        else:
            if parts[:-1]:
                self.repo = '/'.join(parts[:-1])

    def get_auth_endpoint(self) -> None:
        """Get Docker authentication endpoint if required."""
        resp = requests.get(f'https://{self.registry}/v2/', verify=False)
        if resp.status_code == 401:
            self.auth_url = resp.headers['WWW-Authenticate'].split('"')[1]
            try:
                self.reg_service = resp.headers['WWW-Authenticate'].split('"')[3]
            except IndexError:
                self.reg_service = ""

    def get_auth_token(self, content_type: str) -> Dict[str, str]:
        """Get Docker authentication token."""
        resp = requests.get(
            f'{self.auth_url}?service={self.reg_service}&scope=repository:{self.repo}/{self.img}:pull',
            verify=False
        )
        access_token = resp.json()['token']
        return {
            'Authorization': f'Bearer {access_token}',
            'Accept': content_type
        }

    def download_with_progress(self, url: str, file_path: str, description: str) -> None:
        """Download a file with progress display."""
        headers = self.get_auth_token('application/vnd.docker.distribution.manifest.v2+json')
        response = requests.get(url, headers=headers, stream=True, verify=False)
        response.raise_for_status()

        total_size = int(response.headers.get('content-length', 0))
        print(f'{description} total {total_size} B')

        with open(file_path, 'wb') as f:
            for chunk in response.iter_content(chunk_size=8192):
                if chunk:
                    f.write(chunk)

    def fetch_manifest(self) -> Dict:
        """Fetch the image manifest."""
        headers = self.get_auth_token('application/vnd.docker.distribution.manifest.v2+json')
        url = f'https://{self.registry}/v2/{self.repo}/{self.img}/manifests/{self.tag}'
        response = requests.get(url, headers=headers, verify=False)

        if response.status_code != 200:
            self.handle_manifest_error(response)
        
        return response.json()

    def handle_manifest_error(self, response: requests.Response) -> None:
        """Handle manifest fetch errors."""
        print(f'[-] Cannot fetch manifest for {self.repo}/{self.img} [HTTP {response.status_code}]')
        print(response.content)
        
        # Try to get manifest list
        headers = self.get_auth_token('application/vnd.docker.distribution.manifest.list.v2+json')
        url = f'https://{self.registry}/v2/{self.repo}/{self.img}/manifests/{self.tag}'
        response = requests.get(url, headers=headers, verify=False)
        
        if response.status_code == 200:
            print('[+] Manifests found for this tag (use the @digest format to pull the corresponding image):')
            manifests = response.json()['manifests']
            for manifest in manifests:
                platform_info = ', '.join(f'{k}: {v}' for k, v in manifest["platform"].items())
                print(f'{platform_info}, digest: {manifest["digest"]}')
        exit(1)

    def create_image_directory(self) -> None:
        """Create temporary directory for the image."""
        self.image_dir = f'tmp_{self.img}_{self.tag.replace(":", "@")}'
        if os.path.exists(self.image_dir):
            shutil.rmtree(self.image_dir)
        os.mkdir(self.image_dir)
        print(f'Creating image structure in: {self.image_dir}')

    def download_config(self, config_digest: str) -> str:
        """Download image configuration."""
        config_file = f'{self.image_dir}/{config_digest[7:]}.json'
        url = f'https://{self.registry}/v2/{self.repo}/{self.img}/blobs/{config_digest}'
        headers = self.get_auth_token('application/vnd.docker.distribution.manifest.v2+json')
        
        response = requests.get(url, headers=headers, verify=False)
        with open(config_file, 'wb') as f:
            f.write(response.content)
        
        return config_file

    def process_layers(self, layers: List[Dict], config_content: bytes) -> Tuple[List[str], str]:
        """Process all image layers."""
        content = {
            'Config': '',
            'RepoTags': [f'{self.repo}/{self.img}:{self.tag}' if self.repo != 'library' else f'{self.img}:{self.tag}'],
            'Layers': []
        }
        
        parent_id = ''
        empty_json = json.dumps({
            "created": "1970-01-01T00:00:00Z",
            "container_config": {
                "Hostname": "", "Domainname": "", "User": "", 
                "AttachStdin": False, "AttachStdout": False, "AttachStderr": False,
                "Tty": False, "OpenStdin": False, "StdinOnce": False,
                "Env": None, "Cmd": None, "Image": "",
                "Volumes": None, "WorkingDir": "", "Entrypoint": None,
                "OnBuild": None, "Labels": None
            }
        })

        print(f'Total {len(layers)} layers')
        
        for layer in layers:
            parent_id = self.process_layer(layer, parent_id, config_content if layers[-1]['digest'] == layer['digest'] else empty_json)
            content['Layers'].append(f'{parent_id}/layer.tar')
        
        content['Config'] = f'{parent_id}.json'
        return content, parent_id

    def process_layer(self, layer: Dict, parent_id: str, layer_json: str) -> str:
        """Process a single layer."""
        blob_digest = layer['digest']
        layer_id = hashlib.sha256((f'{parent_id}\n{blob_digest}\n').encode('utf-8')).hexdigest()
        layer_dir = os.path.join(self.image_dir, layer_id)
        
        os.mkdir(layer_dir)
        
        # Create VERSION file
        with open(os.path.join(layer_dir, 'VERSION'), 'w') as f:
            f.write('1.0')
        
        # Download and extract layer
        self.download_layer(blob_digest, layer_dir, layer.get('urls', []))
        
        # Create layer JSON
        self.create_layer_json(layer_dir, layer_id, parent_id, layer_json, blob_digest == layer['digest'])
        
        return layer_id

    def download_layer(self, blob_digest: str, layer_dir: str, fallback_urls: List[str]) -> None:
        """Download and extract a layer."""
        gzip_path = os.path.join(layer_dir, 'layer_gzip.tar')
        tar_path = os.path.join(layer_dir, 'layer.tar')
        
        try:
            url = f'https://{self.registry}/v2/{self.repo}/{self.img}/blobs/{blob_digest}'
            self.download_with_progress(url, gzip_path, f'Layer {blob_digest[7:19]}')
        except requests.exceptions.HTTPError:
            if not fallback_urls:
                raise
            self.download_with_progress(fallback_urls[0], gzip_path, f'Layer {blob_digest[7:19]} (fallback)')
        
        # Decompress the layer
        with gzip.open(gzip_path, 'rb') as gz_file, open(tar_path, 'wb') as tar_file:
            shutil.copyfileobj(gz_file, tar_file)
        os.remove(gzip_path)

    def create_layer_json(self, layer_dir: str, layer_id: str, parent_id: str, layer_json: str, is_last_layer: bool) -> None:
        """Create the layer JSON file."""
        json_path = os.path.join(layer_dir, 'json')
        json_data = json.loads(layer_json)
        
        if is_last_layer:
            # Remove unnecessary fields from config
            json_data.pop('history', None)
            json_data.pop('rootfs', None)
            json_data.pop('rootfS', None)  # Handle Microsoft's case insensitivity
        
        json_data['id'] = layer_id
        if parent_id:
            json_data['parent'] = parent_id
        
        with open(json_path, 'w') as f:
            json.dump(json_data, f)

    def create_image_files(self, manifest_content: List[Dict], last_layer_id: str) -> None:
        """Create manifest and repositories files."""
        # Create manifest.json
        with open(os.path.join(self.image_dir, 'manifest.json'), 'w') as f:
            json.dump(manifest_content, f)
        
        # Create repositories
        repo_content = {
            f'{self.repo}/{self.img}' if self.repo != 'library' else self.img: {
                self.tag: last_layer_id
            }
        }
        with open(os.path.join(self.image_dir, 'repositories'), 'w') as f:
            json.dump(repo_content, f)

    def create_image_tar(self) -> str:
        """Create the final image tar archive."""
        tar_name = f'{self.repo.replace("/", "_")}_{self.img}.tar'
        with tarfile.open(tar_name, "w") as tar:
            tar.add(self.image_dir, arcname=os.path.sep)
        shutil.rmtree(self.image_dir)
        return tar_name

    def pull(self) -> str:
        """Main method to pull the Docker image."""
        self.parse_image_reference()
        self.get_auth_endpoint()
        self.create_image_directory()
        
        manifest = self.fetch_manifest()
        config_digest = manifest['config']['digest']
        
        # Download config
        config_file = self.download_config(config_digest)
        with open(config_file, 'rb') as f:
            config_content = f.read()
        
        # Process layers
        manifest_content, last_layer_id = self.process_layers(manifest['layers'], config_content)
        
        # Create image files
        self.create_image_files(manifest_content, last_layer_id)
        
        # Create final tar
        tar_name = self.create_image_tar()
        print(f'\rDocker image pulled: {tar_name}')
        return tar_name


def main():
    if len(sys.argv) != 2:
        print('Usage:\n\tpython docker_pull.py [registry/][repository/]image[:tag|@digest]\n')
        print('Like: \n\tpython docker_pull.py registry.cn-shenzhen.aliyuncs.com/auto_image/pytorch:20.12-py3')
        sys.exit(1)

    try:
        puller = DockerImagePuller(sys.argv[1])
        puller.pull()
    except Exception as e:
        print(f'Error: {str(e)}', file=sys.stderr)
        sys.exit(1)


if __name__ == '__main__':
    main()