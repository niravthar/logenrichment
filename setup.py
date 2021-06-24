# setup.py
import setuptools
REQUIRED_PACKAGES = [
    'google',
    'google-cloud-storage',
    'google-cloud-logging',
    'google-cloud-resource-manager',
    'apache-beam',
    'apache-beam[GCP]'
]

setuptools.setup(
    name='enrichlogs',
    version='0.0.1',
    description='Enrich Logs',
    install_requires=REQUIRED_PACKAGES,
    packages=setuptools.find_packages(),
)
