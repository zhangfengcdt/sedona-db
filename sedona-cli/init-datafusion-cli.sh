
# DataFusion does not provide these components as a library so we have to vendor + modify.
# This will probably need some manual intervention when upgrading to datafusion version
# we are using.
mkdir tmp

curl -L https://github.com/apache/datafusion/archive/refs/tags/44.0.0.zip -o tmp/datafusion.zip
unzip -d tmp tmp/datafusion.zip

cp tmp/datafusion-44.0.0/datafusion-cli/src/*.rs src/
cp tmp/datafusion-44.0.0/datafusion-cli/Cargo.toml .
