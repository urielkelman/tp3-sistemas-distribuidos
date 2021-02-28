import setuptools

setuptools.setup(
    name="tp2_utils",
    version="0.2.2",
    author="Gianmarco Cafferata",
    author_email="giancafferata@hotmail.com",
    packages=setuptools.find_packages(),
    python_requires='>=3.8',
    install_requires=['pika==1.1.0', 'wheel==0.35.1',
                      'pyhash==0.9.3', 'dill==0.3.3', 'numpy==1.20.1']
)
