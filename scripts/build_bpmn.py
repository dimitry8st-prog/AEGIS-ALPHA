from pathlib import Path
from xml.etree import ElementTree as E
from PIL import Image, ImageDraw, ImageFont
import html

OUT=Path(__file__).resolve().parent.parent/'docs'/'bpmn'
OUT.mkdir(parents=True,exist_ok=True)
B='http://www.omg.org/spec/BPMN/20100524/MODEL'
DI='http://www.omg.org/spec/BPMN/20100524/DI'
DC='http://www.omg.org/spec/DD/20100524/DC'
D='http://www.omg.org/spec/DD/20100524/DI'
for p,u in [('bpmn',B),('bpmndi',DI),('dc',DC),('di',D)]: E.register_namespace(p,u)
root=E.Element('{%s}definitions'%B,id='Definitions_Aegis',targetNamespace='https://aegis-alpha.example/processes')
fontpath='/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf'
font=ImageFont.truetype(fontpath,21)
small=ImageFont.truetype(fontpath,18)
titlefont=ImageFont.truetype(fontpath,32)
allmodels=[]

def diagram(pid,title,subtitle,nodes,flows,w=1800,h=920):
    process=E.SubElement(root,'{%s}process'%B,id=pid,name=title,isExecutable='false')
    nd={n[0]:n for n in nodes}
    diag=E.SubElement(root,'{%s}BPMNDiagram'%DI,id='Diagram_'+pid)
    plane=E.SubElement(diag,'{%s}BPMNPlane'%DI,id='Plane_'+pid,bpmnElement=pid)
    im=Image.new('RGB',(w,h),'white'); dr=ImageDraw.Draw(im)
    svg=[f'<svg xmlns="http://www.w3.org/2000/svg" width="{w}" height="{h}" viewBox="0 0 {w} {h}"><rect width="100%" height="100%" fill="white"/><defs><marker id="arrow" markerWidth="10" markerHeight="10" refX="9" refY="5" orient="auto"><path d="M0,0 L10,5 L0,10" fill="#193b63"/></marker></defs>']
    def line(points,color='#193b63',width=2,arrow=False):
        dr.line(points,fill=color,width=width)
        s=' '.join(f'{x},{y}' for x,y in points)
        svg.append(f'<polyline points="{s}" fill="none" stroke="{color}" stroke-width="{width}"'+(' marker-end="url(#arrow)"' if arrow else '')+'/>')
        if arrow:
            import math
            x,y=points[-1]; px,py=points[-2]; a=math.atan2(y-py,x-px)
            tri=[(x,y),(x-12*math.cos(a-.4),y-12*math.sin(a-.4)),(x-12*math.cos(a+.4),y-12*math.sin(a+.4))]
            dr.polygon(tri,fill=color)
    def text(x,y,s,f=font,color='#142d4f',center=False):
        anchor='mm' if center else 'lt'
        dr.text((x,y),s,font=f,fill=color,anchor=anchor)
        svg.append(f'<text x="{x}" y="{y}" font-family="DejaVu Sans,sans-serif" font-size="{f.size}" fill="{color}"'+(' text-anchor="middle" dominant-baseline="middle"' if center else ' dominant-baseline="hanging"')+'>'+html.escape(s)+'</text>')
    text(45,28,title,titlefont);text(45,78,subtitle,small)
    for i,(src,dst,label,points,lp) in enumerate(flows):
        fid=f'{pid}_F{i}'
        fe=E.SubElement(process,'{%s}sequenceFlow'%B,id=fid,sourceRef=src,targetRef=dst,name=label)
        if nd[src][1]=='exclusiveGateway':
            c=E.SubElement(fe,'{%s}conditionExpression'%B); c.text=label
        edge=E.SubElement(plane,'{%s}BPMNEdge'%DI,id='Edge_'+fid,bpmnElement=fid)
        for x,y in points:E.SubElement(edge,'{%s}waypoint'%D,x=str(x),y=str(y))
        line(points,arrow=True)
        if label:text(*lp,label,small)
    for id,kind,label,x,y,bw,bh,extra in nodes:
        attrs=dict(id=id,name=label.replace('\n',' '))
        if kind=='callActivity':attrs['calledElement']=extra['called']
        el=E.SubElement(process,'{%s}%s'%(B,kind),**attrs)
        if extra.get('timer'):
            ev=E.SubElement(el,'{%s}timerEventDefinition'%B)
            E.SubElement(ev,'{%s}timeDuration'%B).text=extra['timer']
        if extra.get('multi'):
            E.SubElement(el,'{%s}multiInstanceLoopCharacteristics'%B,isSequential='true')
        for i,(src,dst,_,_,_) in enumerate(flows):
            if dst==id:E.SubElement(el,'{%s}incoming'%B).text=f'{pid}_F{i}'
            if src==id:E.SubElement(el,'{%s}outgoing'%B).text=f'{pid}_F{i}'
        shape=E.SubElement(plane,'{%s}BPMNShape'%DI,id='Shape_'+id,bpmnElement=id)
        E.SubElement(shape,'{%s}Bounds'%DC,x=str(x),y=str(y),width=str(bw),height=str(bh))
        if 'Event' in kind:
            thick=4 if kind=='endEvent' else 2
            dr.ellipse((x,y,x+bw,y+bh),fill='white',outline='#193b63',width=thick)
            svg.append(f'<ellipse cx="{x+bw/2}" cy="{y+bh/2}" rx="{bw/2}" ry="{bh/2}" fill="white" stroke="#193b63" stroke-width="{thick}"/>')
            if kind=='intermediateCatchEvent':
                dr.ellipse((x+5,y+5,x+bw-5,y+bh-5),outline='#193b63',width=2)
                svg.append(f'<ellipse cx="{x+bw/2}" cy="{y+bh/2}" rx="{bw/2-5}" ry="{bh/2-5}" fill="none" stroke="#193b63" stroke-width="2"/>')
            if extra.get('timer'):
                cx=x+bw/2;cy=y+bh/2
                line([(cx,cy-12),(cx,cy),(cx+10,cy+5)])
            for j,t in enumerate(label.split('\n')):text(x+bw/2,y+bh+25+j*26,t,small,center=True)
        elif 'Gateway' in kind:
            pts=[(x+bw/2,y),(x+bw,y+bh/2),(x+bw/2,y+bh),(x,y+bh/2)]
            dr.polygon(pts,fill='#fff3dc',outline='#193b63');line(pts+[pts[0]])
            svg.append(f'<polygon points="'+ ' '.join(f'{a},{b}' for a,b in pts)+'" fill="#fff3dc" stroke="#193b63" stroke-width="2"/>')
            text(x+bw/2,y+bh/2,'+' if kind=='parallelGateway' else '×',titlefont,center=True)
            for j,t in enumerate(label.split('\n')):text(x+bw/2-(120 if id in ['Gsave','Gretry'] else 0),y-65+j*25,t,small,center=True)
        else:
            thick=4 if kind=='callActivity' else 2
            dr.rounded_rectangle((x,y,x+bw,y+bh),radius=12,fill='#eef5fc',outline='#193b63',width=thick)
            svg.append(f'<rect x="{x}" y="{y}" width="{bw}" height="{bh}" rx="12" fill="#eef5fc" stroke="#193b63" stroke-width="{thick}"/>')
            lines=label.split('\n')
            for j,t in enumerate(lines):
                assert dr.textlength(t,font=font)<bw-12,(id,t)
                text(x+bw/2,y+bh/2+(j-(len(lines)-1)/2)*27-5,t,center=True)
            if extra.get('multi'):text(x+bw/2,y+bh-14,'≡',small,center=True)
    text(45,h-85,'Круг — событие · толстый круг — результат · × — выбор · + — параллельность',small)
    text(45,h-55,'Толстый блок — вызываемый подпроцесс · ≡ — последовательная обработка каждой записи',small)
    svg.append('</svg>')
    (OUT/(pid+'.svg')).write_text('\n'.join(svg),encoding='utf-8')
    im.save(OUT/(pid+'.png'))
    allmodels.append((pid,nodes,flows))

N=lambda id,k,l,x,y,w=220,h=90,**ex:(id,k,l,x,y,w,h,ex)
F=lambda a,b,p,label='',lp=(0,0):(a,b,label,p,lp)

diagram('Process_Aegis','AEGIS Alpha | 1. Сбор исследовательского датасета','Граница: запуск пакета → сохранённый манифест. Прогнозы, торговля и публикация сигналов вне процесса.',[
N('S','startEvent','Запуск\nпакета',70,190,48,48),N('C','scriptTask','Проверить\nконфигурацию',190,170),N('Gcfg','exclusiveGateway','Настройки\nкорректны?',470,190,50,50),N('Fork','parallelGateway','Запустить\nоба потока',610,190,50,50),
N('News','callActivity','Собрать новости\nтип = news',755,125,250,90,called='Process_Collect'),N('Quotes','callActivity','Собрать котировки\nтип = quotes',755,310,250,90,called='Process_Collect'),N('Join','parallelGateway','Оба потока\nзавершены',1080,190,50,50),N('Gready','exclusiveGateway','Пакет\nполон?',1230,190,50,50),N('Manifest','callActivity','Сохранить манифест\nстатус = READY',1390,170,280,90,called='Process_IO'),
N('Gsave','exclusiveGateway','Сохранение\nподтверждено?',1510,465,50,50),N('Ready','endEvent','Датасет готов\nREADY',1690,470,48,48),N('Partial','callActivity','Сохранить манифест\nстатус = PARTIAL',1120,680,280,90,called='Process_IO'),N('Gp','exclusiveGateway','Сохранение\nподтверждено?',1460,700,50,50),N('PartialEnd','endEvent','Пакет неполон\nPARTIAL',1680,700,48,48),N('Incident','scriptTask','Записать диагностику\nдля оператора',210,680,290,90),N('Fail','endEvent','Пакет не готов\nFAILED',620,700,48,48)
],[F('S','C',[(118,214),(190,214)]),F('C','Gcfg',[(410,215),(470,215)]),F('Gcfg','Fork',[(520,215),(610,215)],'Да',(550,180)),F('Gcfg','Incident',[(495,240),(495,590),(355,590),(355,680)],'Нет',(505,410)),F('Fork','News',[(635,190),(635,170),(755,170)]),F('Fork','Quotes',[(635,240),(635,355),(755,355)]),F('News','Join',[(1005,170),(1105,170),(1105,190)]),F('Quotes','Join',[(1005,355),(1105,355),(1105,240)]),F('Join','Gready',[(1130,215),(1230,215)]),F('Gready','Manifest',[(1280,215),(1390,215)],'Да',(1320,180)),F('Gready','Partial',[(1255,240),(1255,680)],'Нет',(1265,475)),F('Manifest','Gsave',[(1535,260),(1535,465)]),F('Gsave','Ready',[(1560,490),(1690,490)],'Да',(1600,460)),F('Gsave','Incident',[(1510,490),(1450,490),(1450,575),(355,575),(355,680)],'Нет',(1360,545)),F('Partial','Gp',[(1400,725),(1460,725)]),F('Gp','PartialEnd',[(1510,725),(1680,725)],'Да',(1560,695)),F('Gp','Incident',[(1485,750),(1485,835),(355,835),(355,770)],'Нет',(1030,805)),F('Incident','Fail',[(500,725),(620,725)])],h=960)

diagram('Process_Collect','AEGIS Alpha | 2. Подпроцесс сбора одного потока','Вызывается для news и quotes. Возвращает итог всегда; отказ одного потока не блокирует параллельное соединение.',[
N('CS','startEvent','Получен\nконтекст потока',70,190,48,48),N('Fetch','callActivity','Получить данные\nчерез адаптер',190,170,260,90,called='Process_IO'),N('Gfetch','exclusiveGateway','Чтение\nуспешно?',520,190,50,50),N('Record','callActivity','Проверить и сохранить\nкаждую запись',680,170,310,100,called='Process_Record',multi=True),N('Summarize','scriptTask','Подсчитать записи\nи покрытие окна',1100,170,280,90),N('CE','endEvent','Итог потока:\nOK / PARTIAL / EMPTY',1510,190,48,48),N('CF','endEvent','Итог потока:\nUNAVAILABLE',750,500,48,48)
],[F('CS','Fetch',[(118,214),(190,214)]),F('Fetch','Gfetch',[(450,215),(520,215)]),F('Gfetch','Record',[(570,215),(680,215)],'Да',(610,180)),F('Gfetch','CF',[(545,240),(545,524),(750,524)],'Нет',(555,370)),F('Record','Summarize',[(990,215),(1100,215)]),F('Summarize','CE',[(1380,215),(1510,215)])],h=780)

diagram('Process_Record','AEGIS Alpha | 3. Обработка записи','Обязательные поля → нормализация UTC → атомарное сохранение. Дубликат не создаёт вторую запись.',[
N('RS','startEvent','Получена\nзапись',70,190,48,48),N('Validate','scriptTask','Проверить поля\nи нормализовать',190,170,270,90),N('Gvalid','exclusiveGateway','Запись\nвалидна?',535,190,50,50),N('Upsert','callActivity','Выполнить UPSERT\nпо уникальному ключу',700,170,310,90,called='Process_IO'),N('Gup','exclusiveGateway','Запись в БД\nподтверждена?',1120,190,50,50),N('RE','endEvent','SAVED / DUPLICATE\n/ UPDATED',1460,190,48,48),N('Quarantine','callActivity','Сохранить в карантин\nс причиной',420,500,290,90,called='Process_IO'),N('Gq','exclusiveGateway','Карантин\nсохранён?',825,520,50,50),N('RQ','endEvent','QUARANTINED',1050,520,48,48),N('Diag','scriptTask','Записать диагностику\nключа и ошибки',1210,650,290,90),N('RF','endEvent','FAILED_RECORD',1630,670,48,48)
],[F('RS','Validate',[(118,214),(190,214)]),F('Validate','Gvalid',[(460,215),(535,215)]),F('Gvalid','Upsert',[(585,215),(700,215)],'Да',(620,180)),F('Gvalid','Quarantine',[(560,240),(560,500)],'Нет',(570,365)),F('Upsert','Gup',[(1010,215),(1120,215)]),F('Gup','RE',[(1170,215),(1460,215)],'Да',(1290,180)),F('Gup','Diag',[(1145,240),(1145,600),(1355,600),(1355,650)],'Нет',(1155,430)),F('Quarantine','Gq',[(710,545),(825,545)]),F('Gq','RQ',[(875,545),(1050,545)],'Да',(930,510)),F('Gq','Diag',[(850,570),(850,695),(1210,695)],'Нет',(960,660)),F('Diag','RF',[(1500,695),(1630,695)])],h=900)

diagram('Process_IO','AEGIS Alpha | 4. Ограниченные повторы операции','Единый подпроцесс для чтения, UPSERT, карантина и манифеста. Максимум 3 попытки; крайний срок пакета — 15 минут.',[
N('IS','startEvent','Вызвана\nоперация',70,190,48,48),N('Init','scriptTask','Установить attempt = 0\nи ключ операции',190,170,300,90),N('Attempt','serviceTask','Увеличить attempt\nвыполнить операцию',610,170,310,90),N('Result','serviceTask','Проверить результат\nи неизвестный исход',1020,170,310,90),N('Gi','exclusiveGateway','Успех\nподтверждён?',1435,190,50,50),N('IE','endEvent','SUCCESS',1650,190,48,48),N('Gretry','exclusiveGateway','Можно\nповторить?',1200,520,50,50),N('Wait','intermediateCatchEvent','Подождать\n30 секунд',850,520,48,48,timer='PT30S'),N('IF','endEvent','FAILURE\nкод + ключ + attempt',1490,520,48,48)
],[F('IS','Init',[(118,214),(190,214)]),F('Init','Attempt',[(490,215),(610,215)]),F('Attempt','Result',[(920,215),(1020,215)]),F('Result','Gi',[(1330,215),(1435,215)]),F('Gi','IE',[(1485,215),(1650,215)],'Да',(1530,180)),F('Gi','Gretry',[(1460,240),(1460,430),(1225,430),(1225,520)],'Нет',(1410,390)),F('Gretry','Wait',[(1200,545),(898,545)],'Да',(1030,510)),F('Gretry','IF',[(1250,545),(1490,545)],'Нет',(1340,510)),F('Wait','Attempt',[(850,544),(765,544),(765,260)],'Следующая попытка',(450,450))],h=800)

E.indent(root)
E.ElementTree(root).write(OUT/'aegis-alpha-data-collection.bpmn',encoding='utf-8',xml_declaration=True)
# Structural check: IDs, references, reachability and termination of the model.
ids=[e.attrib['id'] for e in root.iter() if 'id' in e.attrib]
assert len(ids)==len(set(ids))
for pid,nodes,flows in allmodels:
    ns={n[0] for n in nodes}; adj={n:[] for n in ns};rev={n:[] for n in ns}
    for a,b,*_ in flows: assert a in ns and b in ns;adj[a].append(b);rev[b].append(a)
    def walk(starts,graph):
        seen=set();stack=list(starts)
        while stack:
            n=stack.pop()
            if n not in seen:seen.add(n);stack.extend(graph[n])
        return seen
    starts=[n[0] for n in nodes if n[1]=='startEvent']; ends=[n[0] for n in nodes if n[1]=='endEvent']
    assert walk(starts,adj)==ns and walk(ends,rev)==ns,pid
    for n in nodes:
        if n[1]=='exclusiveGateway': assert all(f[2] for f in flows if f[0]==n[0])
        if n[1]=='callActivity':assert n[-1]['called'] in [m[0] for m in allmodels]
print('4 diagrams rendered; BPMN XML IDs, references, reachability and exits checked')
