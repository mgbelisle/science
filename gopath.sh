if [ "$BASH" ]; then
    REPO_ROOT="$(dirname "${BASH_SOURCE[0]}")"
elif [ "$ZSH_NAME" ]; then
    REPO_ROOT="$(dirname "$0")"
else
    echo "Could not find repo root, please let matthew.belisle@workiva.com know what shell you're using and how you're running this"
    exit 1
fi

cd "$REPO_ROOT"
export GOPATH="$(pwd)/go"
echo GOPATH set to "$GOPATH"
cd - > /dev/null

