import collections
import datetime
import requests
import argparse
from sqlalchemy import (create_engine, ForeignKey,
                        Column, String, Integer,
                        UniqueConstraint)
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, relationship


AuthData = collections.namedtuple('User', ('login', 'password'))
Base = declarative_base()


class Project(Base):
    __tablename__ = 'projects'
    project_name = Column(String(30), primary_key=True)
    url = Column(String(50))


class User(Base):
    __tablename__ = 'users'
    id = Column(Integer, primary_key=True)
    user_name = Column(String(50))
    project_name = Column(String(30), ForeignKey('projects.project_name'))
    login = Column(String(50))
    password = Column(String(50))

    logs = relationship('Log')
    UniqueConstraint(user_name, project_name, name='uix_1')
    UniqueConstraint(login)


class Log(Base):
    __tablename__ = 'logs'
    id = Column(Integer, primary_key=True)
    login = Column(String(50), ForeignKey('users.login'))
    date = Column(String(50))
    comment = Column(String)
    time_spent = Column(String(50))
    project_key = Column(String(40))
    issue_key = Column(String(40))
    issue_summary = Column(String)


def make_requests(host, resource_name, user, *, json=None,
                  api_version='latest', api_name='api', **kwargs):
    request_url = f'https://{host}/rest/{api_name}/{api_version}/{resource_name}'
    if kwargs:
        request_url += '?'
        for key, value in kwargs.items():
            request_url += f'{key}={value}&'
    if json:
        request = requests.post(url=request_url, json=json, auth=(user.login, user.password))
    else:
        request = requests.get(url=request_url, auth=(user.login, user.password))
    if request.status_code >= 300:
        raise Exception(f'Requests error. {request.status_code}.\nMessage: {request.text}')
    return request.json()


def db_init(db_name):
    user = 'postgres'
    password = 'postgresql123'
    host = '127.0.0.1'
    port = '5432'
    db_string = f'postgresql://{user}:{password}@{host}:{port}/{db_name}'

    engine = create_engine(db_string)
    Session = sessionmaker(bind=engine)
    session = Session()
    Base.metadata.create_all(engine)

    try:
        project1 = Project(project_name='ssp', url='jira.sib-soft.ru/jira')
        project2 = Project(project_name='bss', url='jira.bssys.com')
        session.add(project1)
        session.add(project2)
        session.commit()
    except:
        print('Данные уже есть в таблице')
        session.rollback()

    try:
        user1 = User(user_name='max', project_name='ssp', login='denisenkoda', password='')
        user2 = User(user_name='stas', project_name='ssp', login='shkitinsy', password='')
        user3 = User(user_name='kimpa', project_name='ssp', login='kimpa', password='3010352Qr')
        user4 = User(user_name='max', project_name='bss', login='den', password='')
        session.add(user1)
        session.add(user2)
        session.add(user3)
        session.add(user4)
        session.commit()
    except:
        print('Данные уже есть в таблице')
        session.rollback()

    return Session


def get_configs(session, from_projects, to_project, main_user):
    session = session()

    hosts_query = session.query(Project)
    hosts = {h.project_name: h.url for h in hosts_query}

    from_config = []

    for proj in from_projects:
        proj_dict = {}
        proj_dict['host'] = hosts[proj]
        proj_dict['user'] = main_user
        workers_query = session.query(User).filter(User.project_name == proj)
        workers = [AuthData(u.login, u.password) for u in workers_query]
        proj_dict['workers'] = workers
        from_config.append(proj_dict)

    to_config = {}
    to_config['host'] = hosts[to_project]
    workers_query = session.query(User).filter(User.project_name == to_project)
    workers = {u.user_name: AuthData(u.login, u.password) for u in workers_query}
    to_config['workers'] = workers

    return from_config, to_config


def load_worklog_from_jira(config_list, from_worklog_date):
    logs = []
    for host_config in config_list:
        workers = tuple(worker.login for worker in host_config['workers'])
        jql = f'worklogAuthor IN {workers} AND worklogDate >= {from_worklog_date}'
        issues = make_requests(host_config['host'], 'search', host_config['user'],
                               json={
                                   'jql': jql, 'startAt': 0, 'maxResults': 1500,
                                   'fields': ['summary', 'project']
                               })
        for issue in issues['issues']:
            wl = make_requests(host_config['host'], f'issue/{issue["key"]}/worklog', host_config['user'])
            logs += [{'issue': issue, 'log': log} for log in wl['worklogs'] if log['author']['name'] in workers and
                     log['created'].split('T')[0] >= from_worklog_date]
    return logs


def get_crucial_from_logs(session, logs):
    session = session()
    crucial_data = []
    for log in logs:
        issue, log = log['issue'], log['log']
        username_query = session.query(User).filter(User.login == log['author']['name']).first()
        user_name = username_query.user_name
        crucial_data.append({'id': log['id'], 'user_name': user_name, 'started': log['started'],
                             'timeSpent': log['timeSpent'], 'comment': log['comment'],
                             'project_key': issue['fields']['project']['key'],
                             'issue_key': issue['key'], 'issue_summary': issue['fields']['summary']})
    return crucial_data


def save_worklog_to_jira(session, project, config, crucial_data, issue_name):
    session = session()
    count = 0
    written_logs_query = session.query(Log)
    written_logs = [log.id for log in written_logs_query]
    for data in crucial_data:
        login_query = session.query(User).filter(User.user_name == data['user_name']).\
            filter(User.project_name == project).first()
        login = login_query.login

        item = Log(id=data['id'], login=login, date=data['started'],
                   time_spent=data['timeSpent'], comment=data['comment'],
                   project_key=data['project_key'], issue_key=data['issue_key'],
                   issue_summary=data['issue_summary'])
        comment = f"Работа по проекту {data['project_key']}. " + \
                  f"Запрос {data['issue_key']} {data['issue_summary']}.\n" + \
                  f"{data['comment']}"

        if int(data['id']) not in written_logs:
            make_requests(config['host'], f'issue/{issue_name}/worklog',
                          config['workers'][data['user_name']],
                          json={
                              'comment': comment,
                              'started': data['started'],
                              'timeSpent': data['timeSpent']
                          })
            try:
                session.add(item)
                session.commit()
                print(f"Лог {data['id']} успешно записан")
            except:
                session.rollback()
                print(f"Ошибка при сохранении {data['id']}")
            count += 1
    return count


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Загрузка логов из BSS в SSP.')
    parser.add_argument('-db', '--database', type=str, required=True,
                        help='имя используемой базы данных.')
    parser.add_argument('-d', '--date', type=str,
                        default=str(datetime.date.today() - datetime.timedelta(weeks=1)),
                        help='с какой даты запрашивать логи. Формат YYYY-MM-DD.')
    parser.add_argument('-i', '--issue', type=str, required=True,
                        help='задачка, в которую будут сохранены логи.')
    args = parser.parse_args()
    db_session = db_init(args.database)

    # из каких отделов собирать логи
    from_projects = ('ssp',)
    # в задачу какого отдела писать логи
    to_project = 'ssp'
    # пользователь, под которым будут собираться логи
    main_user_login = 'kimpa'
    main_user_password = '3010352Qr'
    main_user = AuthData(main_user_login, main_user_password)

    from_configs, to_config = get_configs(db_session, from_projects, to_project, main_user)
    print('База данных:', args.database)
    print('Дата начала сбора:', args.date)
    print('Задачка, в которую пишутся логи:', args.issue)
    print('Начинаю собирать логи...')
    logs = load_worklog_from_jira(from_configs, args.date)
    print('Логи собраны.')
    crucial_data = get_crucial_from_logs(db_session, logs)

    print('Отправка логов на сервер...')
    count = save_worklog_to_jira(db_session, to_project, to_config, crucial_data, args.issue)
    print('Сделано записей:', count)
