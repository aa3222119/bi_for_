### 打包成可执行文件
    /usr/local/python374/bin/pyinstaller -F console_.py --hidden-import sklearn.utils._cython_blas --hidden-import sklearn.neighbors.quad_tree --hidden-import sklearn.neighbors.typedefs --hidden-import sklearn.tree --hidden-import sklearn.tree._utils
### app
    ./dist/console_ eval 'user_profile_predict(with_init=1)'
    ./dist/console_ eval 'user_profile_predict()' 
    or ./dist/console_ cons user_profile_predict

###
