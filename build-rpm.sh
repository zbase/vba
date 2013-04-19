#!/bin/sh
FILES="
vba        \
"
vba_specfile=vba-dma.spec
vbs_version=`sed -n 's/^Version:[ ]*//p' $vba_specfile`
package_name="vba-dma"
topdir=`pwd`/_rpmbuild

rm -rf $topdir 2>/dev/null

mkdir -p $topdir/{SRPMS,RPMS,BUILD,SOURCES,SPECS}
mkdir -p $topdir/$package_name

cp -ar $FILES $topdir/$package_name && \
cp $vba_specfile $topdir/SPECS && \
echo "Creating source tgz..." && \
tar -czv --exclude=.svn -f $topdir/SOURCES/$package_name.tgz -C $topdir $package_name && \
echo "Building rpm ..." && \
echo "Top dir is: $topdir"
rpmbuild --define="_topdir $topdir" -ba $vba_specfile && \
cp $topdir/SRPMS/*.rpm . && \
cp $topdir/RPMS/*/*.rpm .
rm -rf $topdir
