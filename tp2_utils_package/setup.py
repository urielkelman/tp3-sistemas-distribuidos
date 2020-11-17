import setuptools

setuptools.setup(
    name="tp2_utils",
    version="0.0.1",
    author="Gianmarco Cafferata",
    author_email="giancafferata@hotmail.com",
    packages=['rabbit_utils'],
    python_requires='>=3.8',
    install_requires=['pika==1.1.0', 'wheel==0.35.1', 'pyhash==0.9.3']
)
