from setuptools import setup, find_packages

setup(
    name="ingestor-framework",
    version="1.0.0",
    author="David Caicedo",
    author_email="davidcai@ucm.es",
    description="Framework de Ingesta para el curso de Ingestas",
    long_description="Framework de Ingesta para el curso de Ingestas",
    long_description_content_type="text/markdown",
    url="https://github.com/caicedodavid",
    python_requires=">=3.8",
    packages=find_packages(),
    package_data={"motor_ingesta": ["resources/*.csv"]}
)
