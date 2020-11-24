Git学习

--Git命令
初始化git仓库 git init
查看远程库别名 git remote -v
起别名 git remote add 别名 远程库地址
添加文件:git add 
提交文件:git commit -m(注释) ""
查看工作去和暂存区的状态:git status
查看提交的log:git log

  当历史记录过多时有分页的效果
  下一页:空格
  上一页:b
  退出:q
  方式1: git log --pretty=oneline
  方式2: git log --oneline
  方式3: git reflog
reset 命令:前进或者后退历史版本
git reset --hard 版本号
删除工作去的文件 : rm 文件名
   删除完成之后再次执行同步到本地库
   1.git add
   2.git commit
比较全部文件不同的命令(工作区和暂存区):git diff
比较某个文件不同的命令(工作区和暂存区):git diff 文件名
比较历史版本中不同的命令(工作区和暂存区):git diff 历史版本号 文件名

--分支
在版本控制过程中,使用多条线同时推进多个任务,这里说的多条线就是多个分支
好处:同时多个分支可以并行开发,互不影响,互不当误,提高开发效率
     如果有一个分支功能开发失败,直接删除这个分支就可以,如果开发的可以就合并分支

--操作分支
查看分支 git branch -v
创建分支 git branch 分支name
切换分支 git checkout 分支name
合并分支  1.先切换到主分之
          2.合并分支 git merge 副分支名称(如果有冲突 公司商量决定)
		  3.添加到暂存区
		  4.提交到本地库
		  
--提交远程库操作
推送到远程库 git push
克隆操作(拉取到本地) git clone 远程库地址


--更新本地库操作
--push内容到远程库

git shell 提交代码操作流程
第一次 下载代码到本地库 git clone 远程库地址

1.git pull 别名 分支名
2.git add ./git add 文件名
3.git commit -m "描述"
3.git push 别名 分支名


