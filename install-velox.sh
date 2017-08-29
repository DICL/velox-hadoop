VELOX_PATH=/home/deukyeon/VeloxDFS/build

mvn install:install-file -Dfile=$VELOX_PATH/java/veloxdfs.jar -DgroupId=com.dicl.velox -DartifactId=veloxdfs -Dversion=1.0 -Dpackaging=jar
