import vk_api
import requests
from vk_api.longpoll import VkLongPoll, VkEventType


def main():

    login, password = 'zakozered13@mail.ru', 'Dolphinoz13'
    vk_session = vk_api.VkApi(login, password)

    try:
        vk_session.auth(token_only=True)
    except vk_api.AuthError as error_msg:
        print(error_msg)
        return

    longpoll = VkLongPoll(vk_session, 117)

    for event in longpoll.listen():
        if event.type == VkEventType.USER_TYPING_IN_CHAT:
            print('Печатает ', event.user_id, 'в беседе', event.chat_id)
        elif event.from_chat:
            print(event.user_id, 'в беседе', event.chat_id)
