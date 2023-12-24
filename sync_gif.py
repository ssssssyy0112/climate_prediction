import os
import imageio.v2 as imageio

def create_gif(image_list, gif_name):
    frames = []
    for image_name in image_list:
        frames.append(imageio.imread(image_name))
    imageio.mimsave(gif_name, frames, 'GIF', duration = 1, loop=0)

def main():
    path = './image/'  # 使用的时候把要拼接的gif图片都放到同目录的image文件夹下就可以
    files = os.listdir(path)
    files.sort(key = lambda x: int(x[:-4]))
    image_list = [path + img for img in files]
    gif_name = 'speed_summer.gif'  # 生成的gif的名称，自己改
    create_gif(image_list, gif_name)

if __name__ == "__main__":
    main()
