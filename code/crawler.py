import urllib2
import re
from BeautifulSoup import BeautifulSoup
import urllib2
import json
import time
import traceback
import unicodedata

import threading


def getPostComments(user):
    
    global blogFile
    global blogFileLock
    
    try:
        soup=BeautifulSoup(urllib2.urlopen("http://www.blogster.com/"+user+"/#posts"))
        
        rawstr=str(soup)
        st1=rawstr.split("\n")
        for x in st1:
            if 'bUserBlog.listConstruct' in x:
                break
        y=re.sub(r'\t\t\t\tif\(!bUserBlog.listConstruct\) bUserBlog.init\(\'','',x)
        userid=re.sub(r'\',\'user-blog\'(.*)','',y)
        page=1
        limit=10
        count=0
        sep='\u001F'
        countException=0
    except Exception:
        traceback.print_exc()
        return
    if userid=='':
        return

    while True:
        try:
            result = json.load(urllib2.urlopen('http://www.blogster.com/ai/user-blog-list.api/'+str(int(time.time()))+'?user='+userid+'&page='+str(page)+'&'+'limit='+str(limit)))
            total=int(result['total'])
            if total>0:
                blogs=result['list']
                try:
                    for blog in blogs:
                        timeago=str(blog['timeAgo'])
                        try:
                            title=unicodedata.normalize('NFKD', blog['title']).encode('ascii','ignore')
                        except Exception:
                            title=''
                        url=str(blog['url'])
                        comments=str(blog['comments'])

                        timestamp=str(blog['time'])
                        date=str(blog['date'])
                        id1=str(blog['id'])
                        st=user+sep+id1+sep+timestamp+sep+date+sep+timeago+sep+title+sep+comments+sep+url+sep
                        soup=BeautifulSoup(urllib2.urlopen(url))
                        category,tag,content=getPost(soup)
                        if content!='':
                            content=unicodedata.normalize('NFKD', content).encode('ascii','ignore')
                        st+=str(category)+sep+str(tag)+sep+content+"\n"
                        getComments(soup,user,timestamp,sep,url)
                        blogFile.write(st)
                except Exception:
                    traceback.print_exc()
                    countException+=1
            count+=10
            if count>total:
                break
            page=page+1
        except Exception:
            traceback.print_exc()
            print "Exception related to find the user's page:" +str(user)





def getPost(soup):
# extract post content
    div=soup.find("div", {"id": "user-content-post"})
    divContents=div.findAll(text = True)
    content=''
    for element in divContents:
        if element!='&nbsp;' and element!='\n':
            content+=element
    content=' '.join(content.split('\n'))
    # extracting related taggs
    try:
        for div in soup.findAll('div'):
            for strong in div.findAll('strong'):
                if strong.renderContents()=='Related Tags:':
                    parentDiv=strong.parent
                    break
        for a in parentDiv.findAll('a'):
            relatedTags=a.renderContents()
    except Exception:
        relatedTags="NA"
    # extracting categories
    try:
        for div in soup.findAll('div'):
            for strong in div.findAll('strong'):
                if strong.renderContents()=='Category:':
                    parentDiv=strong.parent
                    break
        for a in parentDiv.findAll('a'):
            category=a.renderContents()
    except Exception:
        category="NA"
    return category,relatedTags,content

# Extract comments
def getComments(soup,user,timestamp,sep,url):
    global commentFile
    global commentFileLock
    try:
        commentBoxes=soup.findAll("div", {"class": "comment-box"})+ soup.findAll("div", {"class": "comment-box comment-box-nested"})
        for comment in commentBoxes:
            try:
                table=comment.find("table")
                rawContent=''
                for element in table:
                    rawContent+=''.join(element.findAll( text = True))
                rawContent=''.join(rawContent.split('\n'))
                commentContent=''.join(rawContent.split('&nbsp;'))
            except Exception:
                traceback.print_exc()
                print 'Exception in a comment: '+ str(url)
            try:
                commentFooter=comment.find("div", {"class": "comment-box-footer"})
                timeMatch=re.search(r'on (.*)at (.*)[a|p]m',str(commentFooter))
                commentTime=timeMatch.group()
            except Exception:
                traceback.print_exc()
                print 'Could not get comment footer'+ str(url)
            try:
                commenter=commentFooter.strong.renderContents()
            except Exception:
                traceback.print_exc()
                print 'Could not get commenter'+ str(url)                
            try:
                commentFile.write(unicodedata.normalize('NFKD', user+sep+timestamp+sep+url+sep+commenter+sep+commentTime+sep+commentContent).encode('ascii','ignore')+'\n')
            except Exception:
                traceback.print_exc()
                print 'Could not write a comment'+ str(url)
    except Exception:
        traceback.print_exc()
        print "In comments: main: "+str(user)+' url: '+str(url)





#### multicrawler for  user selection
import threading
threadNumber=20
sampledNumber=0
sampledNumberLock=threading.Lock()
blogFileLock=threading.Lock()
commentFileLock=threading.Lock()
totalUserCount=0
blogFile=open('/home/imrul/blog/content/blog.txt','wb')
commentFile=open('/home/imrul/blog/content/comment.txt','wb')
class myThread (threading.Thread):
    def __init__(self, threadID, name):
        threading.Thread.__init__(self)
        self.threadID = threadID
        self.name = name
    def run(self):
        global threadNumber
        global sampledNumber
        global sampledNumberLock
        global blogFileLock
        global commentFile
        print "Starting: " + str(self.threadID)
        count=0
        for line in open('/home/imrul/blog/content/seed.txt'): 
            if count%threadNumber==int(self.threadID):
                line=line.split()[0]
                sampledNumber+=1
                if sampledNumber%10==0:
                    print 'Sampling now: '+str(sampledNumber)+', user: '+str(line)
                getPostComments(str(line))
            count+=1         
        print "Exiting " + self.name


threads = []
for thread_number in range (0,threadNumber):
    thread = myThread(thread_number,"Crawler"+str(thread_number))
    thread.start()
    threads.append(thread)

for t in threads:
    t.join()

print 'Exiting Main Thread'
blogFile.close()
commentFile.close()


