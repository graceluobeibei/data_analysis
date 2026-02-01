
# create soft link
src_path=/Users/jupiter/19.git-repo/anthropics/skills/skills
dst_path=../../.opencode/skills

echo "you are going to remove all soft link in $dst_path, input y to continue"
read -p "Are you sure? [y/n]" -n 1 -r
echo    # (optional) move to a new line
if [[ $REPLY =~ ^[Yy]$ ]]
then
  echo "ensure destination exists and remove old soft link ..."
  mkdir -p "$dst_path"
  rm -rf "$dst_path"/*
else
  echo "abort"
  exit 1
fi

cd "$dst_path"
echo "create new soft link ..."
for i in $(ls "$src_path"); do 
  ln -s "$src_path/$i" "$dst_path/$i"
done

echo "done, soft link created:"
ls -lrt $dst_path
