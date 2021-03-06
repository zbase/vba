%define _unpackaged_files_terminate_build 0
Summary:      VBA and DMA component
Name:         vba-dma
Version:      1.0.0.0
Release:      14
Group:        Development/Languages
License:      Apache 2.0
Requires:     python-libevent
BuildRoot:    %{_tmppath}/%{name}-%{version}-root-%(%{__id_u} -n)

Distribution:	VBA

%description
VbucketAgent component

%install
%{__mkdir_p} %{buildroot}/opt/vba
%{__mkdir_p} %{buildroot}/etc/init.d
%{__install} -m 755 %{_topdir}/vba-dma/vba/vba %{buildroot}/etc/init.d
%{__install} -m 755 %{_topdir}/vba-dma/vba/vbamon.sh %{buildroot}/opt/vba
%{__install} -m 755 %{_topdir}/vba-dma/vba/*.py %{buildroot}/opt/vba

%clean
%{__rm} -rf %{buildroot}/

%files
%defattr(-, root, root, -)
/etc/init.d/*
/opt/vba/vbamon.sh
/opt/vba/*.py
/opt/vba/*.pyc
/opt/vba/*.pyo

%changelog
* Wed Apr 19 2013 <nigupta@zynga.com> 1.0.0.0
 second version

* Wed Jan 16 2013 <nigupta@zynga.com> 1.0.0.0
 Initial version
